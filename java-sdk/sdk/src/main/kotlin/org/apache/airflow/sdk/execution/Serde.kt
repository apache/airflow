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
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.execution.comm.DagFileParseRequest
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.SchemaFields
import java.nio.file.InvalidPathException
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

// Serializes Dags to Airflow DagSerialization v3 JSON, mirroring the Go SDK's
// serde (go-sdk/pkg/execution/serde.go), which in turn matches Python's
// DagSerialization output.

// Per-Dag defaults that Python resolves from [core] config when the Dag does
// not override them. The serializer always emits these fields (they have no
// JSON-schema default to omit against), so we fall back to the same values.
private const val DEFAULT_MAX_ACTIVE_TASKS_PER_DAG = 16 // [core] max_active_tasks_per_dag
private const val DEFAULT_MAX_ACTIVE_RUNS_PER_DAG = 16 // [core] max_active_runs_per_dag

private val defaultsMapper = ObjectMapper()

/**
 * Processes a [DagFileParseRequest] by serialising every Dag registered on
 * [bundle] to DagSerialization v3 and returning the result as a
 * DagFileParsingResult body.
 */
internal fun parseDags(
  bundle: Bundle,
  request: DagFileParseRequest,
): Map<String, Any?> {
  val fileloc = request.file ?: ""
  val relativeFileloc = computeRelativeFileloc(fileloc, request.bundlePath)
  val serializedDags =
    bundle.dags.values.map { dag ->
      mapOf(
        "data" to
          mapOf(
            "__version" to 3,
            "dag" to serializeDag(dag, fileloc, relativeFileloc),
          ),
      )
    }
  return linkedMapOf(
    "type" to "DagFileParsingResult",
    "fileloc" to fileloc,
    "serialized_dags" to serializedDags,
  )
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
  val downstream = linkedMapOf<String, MutableList<String>>()
  dag.tasks.forEach { (taskId, def) ->
    def.upstreams.forEach { upstream ->
      downstream.getOrPut(upstream.id) { mutableListOf() } += taskId
    }
  }

  val result =
    linkedMapOf<String, Any?>(
      "dag_id" to dag.id,
      "fileloc" to fileloc,
      "relative_fileloc" to relativeFileloc,
      "timezone" to "UTC",
      "timetable" to serializeTimetable(dag.dagConfig["schedule"] as String?),
      "tasks" to dag.tasks.map { (taskId, def) -> serializeTask(taskId, def, downstream[taskId]) },
      "dag_dependencies" to emptyList<Any?>(),
      "task_group" to serializeTaskGroup(dag.tasks.keys),
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
 *
 * Native Java tasks deliberately emit no `_arg_bindings`: the execution API
 * delivers bindings only for Python `_StubOperator` tasks, and a Java task
 * always executes inside the JVM bundle that already holds its wired inputs,
 * so the runtime resolves them locally.
 */
private fun serializeTask(
  taskId: String,
  def: TaskDef,
  downstream: List<String>?,
): Map<String, Any?> {
  val data =
    linkedMapOf<String, Any?>(
      "task_id" to taskId,
      "task_type" to def.definition.simpleName,
      "_task_module" to def.definition.packageName,
      "language" to "java",
      // Python's operator serializer always emits template_fields (its list
      // value never matches the tuple default it is compared against), so it
      // is unconditional here too. Java tasks have no template fields.
      "template_fields" to emptyList<Any?>(),
    )
  // Emit only config entries that differ from their schema default, mirroring
  // Python BaseSerialization's "omit hard-coded default" behavior. Operator
  // fields are stored unwrapped, so the __type encoding is stripped.
  def.configValues.forEach { (key, value) ->
    if (!matchesSchemaDefault(SchemaFields.TASK[key], value)) {
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

/**
 * Writes Dag-level config onto [data]. Fields with a JSON-schema default
 * (description, dates, tags, fail_fast, ...) are omitted when unset. Fields
 * with no schema default (catchup, disable_bundle_versioning,
 * max_active_tasks, max_active_runs, max_consecutive_failed_dag_runs) are
 * always emitted, because Python's serializer never omits them — it writes
 * the resolved value, falling back to the matching `[core]` config default.
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
    data["tags"] = tags.map { it.toString() }.sorted()
  }
  data["max_active_tasks"] = config["max_active_tasks"] ?: DEFAULT_MAX_ACTIVE_TASKS_PER_DAG
  data["max_active_runs"] = config["max_active_runs"] ?: DEFAULT_MAX_ACTIVE_RUNS_PER_DAG
  data["max_consecutive_failed_dag_runs"] = config["max_consecutive_failed_dag_runs"] ?: 0
  data["catchup"] = config["catchup"] ?: false
  data["disable_bundle_versioning"] = config["disable_bundle_versioning"] ?: false
  // fail_fast and render_template_as_native_obj have schema default false, so
  // Python omits them when false; keep that behavior.
  if (config["fail_fast"] == true) data["fail_fast"] = true
  if (config["render_template_as_native_obj"] == true) data["render_template_as_native_obj"] = true
  config["is_paused_upon_creation"]?.let { data["is_paused_upon_creation"] = it }
}

// TODO: respect [scheduler] create_cron_data_intervals like Python's
// _create_timetable; the JVM bundle cannot read airflow.cfg, so the
// supervisor must send those flags over the coordinator protocol first.
// Mirrors the Go SDK's default-only behavior; tracked at
// https://github.com/apache/airflow/issues/67938
private fun serializeTimetable(schedule: String?): Map<String, Any?> =
  when (schedule) {
    null -> mapOf("__type" to "airflow.timetables.simple.NullTimetable", "__var" to emptyMap<String, Any?>())
    "@once" -> mapOf("__type" to "airflow.timetables.simple.OnceTimetable", "__var" to emptyMap<String, Any?>())
    "@continuous" ->
      mapOf("__type" to "airflow.timetables.simple.ContinuousTimetable", "__var" to emptyMap<String, Any?>())
    else ->
      mapOf(
        "__type" to "airflow.timetables.trigger.CronTriggerTimetable",
        "__var" to
          mapOf(
            "expression" to schedule,
            "timezone" to "UTC",
            "interval" to 0.0,
            "run_immediately" to false,
          ),
      )
  }

/** Creates the flat root task group containing all task IDs. */
private fun serializeTaskGroup(taskIds: Collection<String>): Map<String, Any?> =
  mapOf(
    "_group_id" to null,
    "group_display_name" to "",
    "prefix_group_id" to true,
    "tooltip" to "",
    "ui_color" to "CornflowerBlue",
    "ui_fgcolor" to "#000",
    "children" to taskIds.associateWith { listOf("operator", it) },
    "upstream_group_ids" to emptyList<Any?>(),
    "downstream_group_ids" to emptyList<Any?>(),
    "upstream_task_ids" to emptyList<Any?>(),
    "downstream_task_ids" to emptyList<Any?>(),
  )

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
