/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.sdk.execution

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.GroupEdges
import org.apache.airflow.sdk.GroupExpansion
import org.apache.airflow.sdk.LiteralArg
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskGroupRef
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.TriggerDagRun
import org.apache.airflow.sdk.execution.comm.DagFileParseRequest
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.SchemaFields
import java.nio.file.InvalidPathException
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

// Serializes Dags to Airflow DagSerialization v3 JSON, mirroring the TypeScript
// SDK's serde (ts-sdk/src/coordinator/serde.ts), which in turn matches Python's
// DagSerialization output.

private val defaultsMapper = ObjectMapper()

// Python drops both unless the operator names an email recipient. A Java task
// has no email field to name one, so they are never written.
private val OMITTED_TASK_KEYS = setOf("email_on_failure", "email_on_retry")

/**
 * Processes a [DagFileParseRequest] by serialising every Dag registered on
 * [bundle] to DagSerialization v3 and returning the result as a
 * DagFileParsingResult body.
 *
 * A Dag that cannot be serialized becomes an import error rather than taking
 * the rest of the bundle with it. Airflow keys an import error by the
 * bundle-relative path and holds one row per file, so every failure here is
 * reported under that one key with its Dag named in the message.
 */
internal fun parseDags(
  bundle: Bundle,
  request: DagFileParseRequest,
): Map<String, Any?> {
  val fileloc = request.file ?: ""
  val relativeFileloc = computeRelativeFileloc(fileloc, request.bundlePath)
  val serializedDags = mutableListOf<Map<String, Any?>>()
  val failures = mutableListOf<String>()
  bundle.dags.values.forEach { dag ->
    runCatching { serializeDag(dag, fileloc, relativeFileloc) }
      .onSuccess { serializedDags += mapOf("data" to mapOf("__version" to 3, "dag" to it)) }
      .onFailure { failures += "Dag \"${dag.id}\": ${it.message ?: it.javaClass.name}" }
  }
  return linkedMapOf<String, Any?>(
    "type" to "DagFileParsingResult",
    "fileloc" to fileloc,
    "serialized_dags" to serializedDags,
  ).apply {
    if (failures.isNotEmpty()) this["import_errors"] = mapOf(relativeFileloc to failures.joinToString("\n"))
  }
}

/**
 * Converts a [DagDef] to Airflow DagSerialization v3 format. Required fields are
 * always present; config-driven fields follow the rules in [applyDagConfig]
 * (some always emitted, some only when set).
 */
internal fun serializeDag(
  dag: DagDef,
  fileloc: String,
  relativeFileloc: String,
): Map<String, Any?> {
  // Group edges mean tasks only once the Dag is complete, so they are worked
  // out here rather than carried on the groups themselves.
  val expansion = dag.expandGroupEdges()
  val downstream = linkedMapOf<String, MutableList<String>>()
  dag.tasks.forEach { (taskId, def) ->
    expansion.upstreamsOf(def).forEach { upstream ->
      downstream.getOrPut(upstream) { mutableListOf() } += taskId
    }
  }

  val result =
    linkedMapOf<String, Any?>(
      "dag_id" to dag.id,
      "fileloc" to fileloc,
      "relative_fileloc" to relativeFileloc,
      "timezone" to dagTimezone(dag.dagConfig),
      "timetable" to serializeTimetable(dag.id, dag.dagConfig),
      "tasks" to dag.tasks.map { (taskId, def) -> serializeTask(taskId, def, downstream[taskId]) },
      "dag_dependencies" to serializeDagDependencies(dag),
      "task_group" to serializeTaskGroups(dag, expansion),
      "edge_info" to emptyMap<String, Any?>(),
      "params" to emptyList<Any?>(),
      "deadline" to null,
      "allowed_run_types" to null,
    )
  applyDagConfig(result, dag.dagConfig)
  return result
}

/**
 * Converts one task to the Airflow serialization format. `downstream` is the
 * inverted view of the Dag's upstream edges, sorted for stable JSON.
 */
private fun serializeTask(
  taskId: String,
  def: TaskDef,
  downstream: List<String>?,
): Map<String, Any?> {
  val data = linkedMapOf<String, Any?>("task_id" to taskId)
  val trigger = def.trigger
  if (trigger != null) {
    writeTriggerDagRun(data, trigger)
  } else {
    data["task_type"] = def.definition.simpleName
    data["_task_module"] = def.definition.packageName
    data["language"] = "java"
    // Python's operator serializer always emits template_fields (its list
    // value never matches the tuple default it is compared against), so it
    // is unconditional here too. Java tasks have no template fields.
    data["template_fields"] = emptyList<Any?>()
    // What marks a task whose arguments Airflow resolves per instance for a
    // runtime outside Python, as `@task.stub` does on the Python side.
    // `get_arg_bindings` reads nothing without it.
    data["is_stub"] = true
    argBindings(taskId, def)?.let { data["_arg_bindings"] = it }
  }
  // Lets NotPreviouslySkippedDep re-skip a cleared downstream, as Python's SkipMixin does.
  if (def.decider != null) data["_can_skip_downstream"] = true
  // Emit only config entries that differ from their schema default, mirroring
  // Python BaseSerialization's "omit hard-coded default" behavior, which the Go
  // and TypeScript SDKs mirror too. Operator fields are stored unwrapped, so the
  // __type encoding is stripped. If core grows a task-level fill_config_defaults,
  // every SDK has to keep explicitly set values instead, or an explicit retries=0
  // reads as unset and picks up the configured default.
  def.configValues.forEach { (key, value) ->
    if (key !in OMITTED_TASK_KEYS && !matchesSchemaDefault(SchemaFields.TASK[key], value)) {
      data[key] = unwrapTypeEncoding(serializeValue(value))
    }
  }
  if (!downstream.isNullOrEmpty()) {
    data["downstream_task_ids"] = downstream.sorted()
  }
  return mapOf(
    "__type" to "operator",
    "__var" to data,
  )
}

/** What Python's serializer writes for a `TriggerDagRunOperator`. */
private const val TRIGGER_TASK_TYPE = "TriggerDagRunOperator"
private const val TRIGGER_TASK_MODULE = "airflow.providers.standard.operators.trigger_dagrun"
private const val TRIGGER_UI_COLOR = "#ffefeb"

private val TRIGGER_TEMPLATE_FIELDS =
  listOf(
    "trigger_dag_id",
    "trigger_run_id",
    "logical_date",
    "conf",
    "wait_for_completion",
    "skip_when_already_exists",
  )

/**
 * Writes a task declared from a [TriggerDagRun] as Python writes a
 * `TriggerDagRunOperator`, so the Airflow UI and the Dag dependency graph
 * treat it as one.
 *
 * Python writes an operator's template fields and leaves its other parameters
 * out, because a Python Dag file holds them. A Java Dag has no such file, so
 * every setting the author made is written under the name Python's parameter
 * carries. The runtime runs the task from the Dag in its own bundle and reads
 * none of this back.
 */
private fun writeTriggerDagRun(
  data: MutableMap<String, Any?>,
  trigger: TriggerDagRun,
) {
  data["task_type"] = TRIGGER_TASK_TYPE
  data["_task_module"] = TRIGGER_TASK_MODULE
  data["ui_color"] = TRIGGER_UI_COLOR
  data["template_fields"] = TRIGGER_TEMPLATE_FIELDS
  data["template_fields_renderers"] = mapOf("conf" to "py")
  data["_operator_extra_links"] = mapOf("Triggered DAG" to "_link_TriggerDagRunLink")
  data["trigger_dag_id"] = trigger.dagId

  val settings = trigger.settings
  // Python's logical_date defaults to NOTSET, which lets the operator pick the
  // trigger time, and writes that sentinel as its name. A template field holds
  // a datetime as str(datetime), not as a {"__type": "datetime"} object.
  data["logical_date"] = settings["logical_date"]?.let { pythonStr(it) } ?: "NOTSET"
  // Python writes these two template fields whatever they hold.
  data["wait_for_completion"] = settings["wait_for_completion"] ?: false
  data["skip_when_already_exists"] = settings["skip_when_already_exists"] ?: false
  settings["trigger_run_id"]?.let { data["trigger_run_id"] = it }
  settings["conf"]?.let { data["conf"] = it }
  // A field that is not a template field is read back with
  // BaseSerialization.deserialize, which returns a datetime only for the
  // type-encoded form.
  settings["run_after"]?.let { data["run_after"] = serializeValue(it) }
  settings["reset_dag_run"]?.takeIf { it == true }?.let { data["reset_dag_run"] = true }
  settings["poke_interval"]?.let { data["poke_interval"] = (it as Duration).seconds.toInt() }
  settings["allowed_states"]?.let { data["allowed_states"] = it }
  settings["failed_states"]?.let { data["failed_states"] = it }
  settings["fail_when_dag_is_paused"]?.takeIf { it == true }?.let { data["fail_when_dag_is_paused"] = true }
  settings["note"]?.let { data["note"] = it }
  settings["deferrable"]?.let { data["deferrable"] = it }
}

/** `str(datetime)` in UTC, which is how Python writes a datetime in a template field. */
private fun pythonStr(value: Any): String {
  val instant = if (value is OffsetDateTime) value.toInstant() else value as Instant
  val utc = instant.atOffset(ZoneOffset.UTC)
  val micros = utc.nano / 1000
  val seconds = utc.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"))
  return if (micros == 0) "$seconds+00:00" else "$seconds.%06d+00:00".format(micros)
}

/**
 * The Dags this one triggers, which is what the UI draws the dependency graph
 * between Dags from, sorted as Python sorts them.
 */
private fun serializeDagDependencies(dag: DagDef): List<Map<String, Any?>> =
  dag.tasks
    .filterValues { it.trigger != null }
    .map { (taskId, def) ->
      mapOf(
        "source" to dag.id,
        "target" to def.trigger!!.dagId,
        // Python labels the dependency with the task's display name, which a Java task cannot set
        // yet, and falls back to the task ID as this does.
        "label" to taskId,
        "dependency_type" to "trigger",
        "dependency_id" to taskId,
      )
    }
    // Python sorts DagDependency as the ordered dataclass it is, field by field.
    .sortedWith(
      compareBy(
        { it["source"] as String },
        { it["target"] as String },
        { it["label"] as String },
        { it["dependency_type"] as String },
        { it["dependency_id"] as String },
      ),
    )

/**
 * The task's arguments as the binding spec Airflow records, one entry per
 * argument in the order the Dag's call passed them, as
 * [ADR-0007](../../adr/lang-sdk/0007-taskflow-across-language-boundary.md)
 * defines it.
 *
 * An upstream's handle becomes an `xcom` binding naming that task, and
 * anything else a `literal` carrying the value. `value_schema` is left out: it
 * constrains the decode side, and a Java task decodes into the type its own
 * parameter declares.
 *
 * A Java task reads its arguments from the Dag in its own bundle rather than
 * from this spec, so what it carries is what Airflow shows and what a change
 * to an argument is seen in.
 *
 * Null for a task the Dag called with no arguments, which needs no spec.
 */
private fun argBindings(
  taskId: String,
  def: TaskDef,
): List<Map<String, Any?>>? {
  // A Dag wired by hand through Refs names no argument, and a spec without
  // names binds nothing, so it is left out rather than written half-filled.
  if (def.inputNames.size != def.inputs.size || def.inputs.isEmpty()) return null
  return def.inputNames.zip(def.inputs) { name, input ->
    when (input) {
      is TaskRef<*> -> mapOf("name" to name, "kind" to "xcom", "task_id" to input.def.id)
      is LiteralArg<*> ->
        mapOf("name" to name, "kind" to "literal", "value" to plainJson(input.value, name, taskId))
    }
  }
}

/**
 * [value] as the JSON the binding spec travels as, rejecting anything that has
 * no JSON form.
 *
 * The spec is part of the serialized Dag, so a literal Airflow cannot store is
 * refused where the Dag is written rather than where the task reads it.
 */
private fun plainJson(
  value: Any?,
  name: String,
  taskId: String,
): Any? =
  when (value) {
    null, is String, is Boolean -> value
    is Double ->
      value.takeIf { it.isFinite() }
        ?: throw IllegalArgumentException(
          "Argument '$name' of task '$taskId' is $value, which JSON has no form for; pass it as a string",
        )
    is Float -> plainJson(value.toDouble(), name, taskId)
    is Int, is Long, is Short, is Byte -> value
    is Collection<*> -> value.map { plainJson(it, name, taskId) }
    is Array<*> -> value.map { plainJson(it, name, taskId) }
    is Map<*, *> ->
      value.entries.associate { (key, entry) ->
        require(key is String) { "Argument '$name' of task '$taskId' has a map key that is not a string" }
        key to plainJson(entry, name, taskId)
      }
    else ->
      throw IllegalArgumentException(
        "Argument '$name' of task '$taskId' is a ${value.javaClass.name}, which has no JSON form; the " +
          "Dag's call arguments travel as JSON, so pass a string, number, boolean, list, or map",
      )
  }

/**
 * Writes Dag-level config onto [data], leaving out every field the Dag did
 * not set. That includes the fields Python reads from Airflow's config
 * (max_active_tasks, max_active_runs, max_consecutive_failed_dag_runs,
 * catchup, disable_bundle_versioning): Airflow fills those in from its own
 * config when it receives the Dag.
 */
private fun applyDagConfig(
  data: MutableMap<String, Any?>,
  config: Map<String, Any>,
) {
  listOf("description", "dag_display_name", "doc_md", "start_date", "end_date", "dagrun_timeout").forEach { key ->
    config[key]?.let { data[key] = unwrapTypeEncoding(serializeValue(it)) }
  }
  (config["tags"] as? List<*>)?.let { tags ->
    // Python stores tags in a set and serializes them sorted (for a stable
    // dag_hash); mirror that regardless of registration order.
    data["tags"] = tags.map { it.toString() }.distinct().sorted()
  }
  listOf(
    "max_active_tasks",
    "max_active_runs",
    "max_consecutive_failed_dag_runs",
    "catchup",
    "disable_bundle_versioning",
  ).forEach { key -> config[key]?.let { data[key] = it } }
  // fail_fast and render_template_as_native_obj have schema default false, so
  // Python omits them when false; keep that behavior.
  if (config["fail_fast"] == true) data["fail_fast"] = true
  if (config["render_template_as_native_obj"] == true) data["render_template_as_native_obj"] = true
  config["is_paused_upon_creation"]?.let { data["is_paused_upon_creation"] = it }
}

// TODO: respect [scheduler] create_cron_data_intervals like Python's
// _create_timetable; the JVM bundle cannot read airflow.cfg, so the
// supervisor must send those flags over the coordinator protocol first.
// The same gap applies to [core] default_timezone, which Python's
// _extract_tz uses for a Dag with no start date; this uses UTC.
// The TypeScript SDK waits on the same flag; tracked at
// https://github.com/apache/airflow/issues/67938
private fun serializeTimetable(
  dagId: String,
  config: Map<String, Any>,
): Map<String, Any?> =
  when (val schedule = config["schedule"] as String?) {
    null -> mapOf("__type" to "airflow.timetables.simple.NullTimetable", "__var" to emptyMap<String, Any?>())
    "@once" -> mapOf("__type" to "airflow.timetables.simple.OnceTimetable", "__var" to emptyMap<String, Any?>())
    "@continuous" ->
      mapOf("__type" to "airflow.timetables.simple.ContinuousTimetable", "__var" to emptyMap<String, Any?>())
    else -> {
      val expression = CRON_PRESETS[schedule] ?: schedule
      require(isCronExpression(expression)) {
        "Schedule '$schedule' of Dag '$dagId' is not a cron expression or a preset " +
          "(${(CRON_PRESETS.keys + CRON_ALIASES).joinToString()}, @once, @continuous); a schedule the " +
          "scheduler cannot parse would leave the Dag unschedulable"
      }
      mapOf(
        "__type" to "airflow.timetables.trigger.CronTriggerTimetable",
        "__var" to
          mapOf(
            "expression" to expression,
            "timezone" to dagTimezone(config),
            "interval" to 0.0,
            "run_immediately" to false,
          ),
      )
    }
  }

/**
 * The Dag's timezone, as Python's `encode_timezone` writes it: `"UTC"` for a
 * zero offset, otherwise the offset in seconds.
 *
 * Python takes it from `start_date`, so a cron schedule runs in the zone the
 * Dag's start date was written in. A Dag with no start date runs in UTC.
 */
private fun dagTimezone(config: Map<String, Any>): Any =
  (config["start_date"] as? OffsetDateTime)
    ?.offset
    ?.totalSeconds
    ?.takeIf { it != 0 }
    ?: "UTC"

/**
 * Presets expanded the way `CronMixin.__init__` expands them, so the
 * serialized expression is the one Python records, which the Dag's summary and
 * its hash are both taken from. Mirrors `airflow.utils.dates.cron_presets`.
 */
private val CRON_PRESETS =
  mapOf(
    "@hourly" to "0 * * * *",
    "@daily" to "0 0 * * *",
    "@weekly" to "0 0 * * 0",
    "@monthly" to "0 0 1 * *",
    "@quarterly" to "0 0 1 */3 *",
    "@yearly" to "0 0 1 1 *",
  )

/** One value in a cron field: a number, `*`, `?`, or a three-letter month or weekday name. */
private const val CRON_VALUE = "(\\d+|\\*|\\?|[A-Z]{3})"

/**
 * One comma-separated element of a cron field: a value or a range, either
 * stepped, with croniter's `L`, `W` and `#` qualifiers.
 */
private val CRON_ELEMENT =
  Regex("$CRON_VALUE(-$CRON_VALUE)?([/#]\\d+)?[LW]*|L(-\\d+)?|LW", RegexOption.IGNORE_CASE)

/**
 * Aliases croniter accepts that `cron_presets` does not expand, so Python
 * stores them unexpanded and so does this.
 */
private val CRON_ALIASES = setOf("@midnight", "@annually")

/**
 * Whether [expression] has the shape croniter accepts: five or six
 * space-separated fields of cron characters, or an `@` alias.
 *
 * A shape check, not a parse: croniter validates the ranges, and repeating
 * that here would be a second implementation to keep in step. What it catches
 * is prose, such as `"every tuesday"`, which would otherwise be written into a
 * Dag the scheduler then fails to build a timetable for.
 */
private fun isCronExpression(expression: String): Boolean {
  val trimmed = expression.trim()
  if (trimmed.startsWith("@")) return trimmed in CRON_PRESETS || trimmed in CRON_ALIASES
  val fields = trimmed.split(Regex("\\s+"))
  return fields.size in 5..6 &&
    fields.all { field -> field.split(',').all { CRON_ELEMENT.matches(it) } }
}

/**
 * Serializes the Dag's task groups as Python's `TaskGroupSerialization` does:
 * a root group holding the tasks in no group and the top-level groups, each
 * group nesting its own tasks and groups.
 */
private fun serializeTaskGroups(
  dag: DagDef,
  expansion: GroupExpansion,
): Map<String, Any?> {
  val grouped = dag.groups.values.flatMapTo(mutableSetOf()) { it.taskIds }
  return taskGroupObject(
    null,
    dag.tasks.keys.filterNot { it in grouped },
    dag.groups.values.filterNot { '.' in it.id },
    expansion,
  )
}

/** One group object: the root when [group] is null, otherwise a nested one. */
private fun taskGroupObject(
  group: TaskGroupRef?,
  taskIds: List<String>,
  children: List<TaskGroupRef>,
  expansion: GroupExpansion,
): Map<String, Any?> =
  mapOf(
    // The local segment: Python's TaskGroup stores the ID it was given, and
    // rebuilds the full one from where the group sits in the tree.
    "_group_id" to group?.id?.substringAfterLast('.'),
    "group_display_name" to "",
    "prefix_group_id" to true,
    "tooltip" to "",
    "ui_color" to "CornflowerBlue",
    "ui_fgcolor" to "#000",
    "children" to
      taskIds.associateWith { listOf("operator", it) } +
      children.associate {
        it.id to listOf("taskgroup", taskGroupObject(it, it.taskIds, it.children, expansion))
      },
    "upstream_group_ids" to group.edges(expansion).upstreamGroupIds.sorted(),
    "downstream_group_ids" to group.edges(expansion).downstreamGroupIds.sorted(),
    "upstream_task_ids" to group.edges(expansion).upstreamTaskIds.sorted(),
    "downstream_task_ids" to group.edges(expansion).downstreamTaskIds.sorted(),
  )

/** The edges this group records for itself; the root group records none. */
private fun TaskGroupRef?.edges(expansion: GroupExpansion): GroupEdges = this?.let { expansion.edgesOf(it.id) } ?: GroupEdges()

/**
 * Recursively serializes a value with Airflow's type/var encoding, matching
 * Python's `BaseSerialization.serialize()` output: primitives pass through,
 * date-times become `{"__type": "datetime", "__var": epoch_seconds}`,
 * durations `{"__type": "timedelta", "__var": total_seconds}`, and maps
 * `{"__type": "dict", "__var": {...}}`.
 */
internal fun serializeValue(value: Any?): Any? =
  when (value) {
    null -> null
    is String, is Boolean, is Int, is Long, is Double -> value
    is Byte, is Short -> (value as Number).toInt()
    is Float -> value.toDouble()
    is OffsetDateTime -> serializeValue(value.toInstant())
    is Instant ->
      mapOf(
        "__type" to "datetime",
        "__var" to value.epochSecond + value.nano / 1e9,
      )
    is Duration ->
      mapOf(
        "__type" to "timedelta",
        "__var" to value.toNanos() / 1e9,
      )
    is Map<*, *> ->
      mapOf(
        "__type" to "dict",
        "__var" to value.entries.associate { (k, v) -> k.toString() to serializeValue(v) },
      )
    is List<*> -> value.map(::serializeValue)
    is Array<*> -> value.map(::serializeValue)
    else -> value
  }

/**
 * Extracts the `__var` part from a type-encoded value: in Python's
 * `serialize_to_json`, non-decorated fields are serialized then unwrapped.
 */
internal fun unwrapTypeEncoding(value: Any?): Any? {
  val map = value as? Map<*, *> ?: return value
  if ("__type" !in map) return value
  return if ("__var" in map) map["__var"] else value
}

/** Whether a config value equals the schema default and can be omitted. */
private fun matchesSchemaDefault(
  field: Field?,
  value: Any,
): Boolean {
  val defaultJson = field?.defaultJson ?: return false
  val node = defaultsMapper.readTree(defaultJson)
  return when (value) {
    is String -> node.isTextual && node.asText() == value
    is Boolean -> node.isBoolean && node.asBoolean() == value
    is Number -> node.isNumber && node.asDouble() == value.toDouble()
    is Duration -> node.isNumber && node.asDouble() == value.toNanos() / 1e9
    else -> false
  }
}

private fun computeRelativeFileloc(
  fileloc: String,
  bundlePath: String?,
): String {
  if (fileloc.isEmpty()) return ""
  if (bundlePath.isNullOrEmpty()) return "."
  return try {
    Paths
      .get(bundlePath)
      .relativize(Paths.get(fileloc))
      .toString()
      .ifEmpty { "." }
  } catch (e: InvalidPathException) {
    "."
  } catch (e: IllegalArgumentException) {
    "."
  }
}
