package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Dag
import org.apache.airflow.sdk.Task
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/**
 * Serialization logic decoupled from user-facing SDK classes.
 *
 * Produces output compatible with Python Airflow's DagSerialization format (version 3).
 */
typealias Serialized = Map<String, Any?>

private object SerdeScope

private val logger = Logger(SerdeScope::class)

// ---------------------------------------------------------------------------
// Value encoding — matches Python's BaseSerialization.serialize
// ---------------------------------------------------------------------------

/**
 * Recursively serialize a value with Airflow's type/var encoding.
 *
 * Primitives pass through; complex types are wrapped in {"__type": ..., "__var": ...}.
 * This matches the Python BaseSerialization.serialize() output exactly:
 * - dict  -> {"__type": "dict",      "__var": {k: serialize(v), ...}}
 * - set   -> {"__type": "set",       "__var": [sorted items]}
 * - datetime -> {"__type": "datetime",  "__var": epoch_seconds}
 * - timedelta -> {"__type": "timedelta", "__var": total_seconds}
 */
internal fun serializeValue(value: Any?): Any? =
  when (value) {
    null -> null
    is String, is Boolean, is Int, is Long, is Float, is Double -> value
    is Instant ->
      mapOf(
        "__type" to "datetime",
        "__var" to (value.epochSecond.toDouble() + value.nano.toDouble() / 1_000_000_000.0),
      )
    is Duration ->
      mapOf(
        "__type" to "timedelta",
        "__var" to (value.toMillis().toDouble() / 1000.0),
      )
    is Map<*, *> ->
      mapOf(
        "__type" to "dict",
        "__var" to value.entries.associate { (k, v) -> k.toString() to serializeValue(v) },
      )
    is Set<*> -> {
      val items = value.map { serializeValue(it) }
      mapOf(
        "__type" to "set",
        "__var" to
          try {
            items.sortedBy { it?.toString() ?: "" }
          } catch (_: Exception) {
            items
          },
      )
    }
    is List<*> -> value.map { serializeValue(it) }
    else -> value.toString()
  }

/**
 * Unwrap a single level of type encoding, extracting the "__var" part.
 *
 * In Python's serialize_to_json, non-decorated fields are serialized then unwrapped:
 *   value = cls.serialize(value)
 *   if isinstance(value, dict) and Encoding.TYPE in value:
 *       value = value[Encoding.VAR]
 */
private fun unwrapTypeEncoding(value: Any?): Any? =
  if (value is Map<*, *> && "__type" in value && "__var" in value) {
    value["__var"]
  } else {
    value
  }

// ---------------------------------------------------------------------------
// Timetable serialization
// ---------------------------------------------------------------------------

private fun serializeTimetable(schedule: String?): Serialized =
  when (schedule) {
    null ->
      mapOf(
        "__type" to "airflow.timetables.simple.NullTimetable",
        "__var" to emptyMap<String, Any>(),
      )
    "@once" ->
      mapOf(
        "__type" to "airflow.timetables.simple.OnceTimetable",
        "__var" to emptyMap<String, Any>(),
      )
    "@continuous" ->
      mapOf(
        "__type" to "airflow.timetables.simple.ContinuousTimetable",
        "__var" to emptyMap<String, Any>(),
      )
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

// ---------------------------------------------------------------------------
// Task serialization
// ---------------------------------------------------------------------------

private fun Class<out Task>.serialize(
  id: String,
  dependants: Collection<String>?,
): Serialized {
  val data =
    mutableMapOf<String, Any>(
      "task_id" to id,
      "task_type" to simpleName,
      "_task_module" to name.substringBeforeLast('.'),
      "language" to "java",
    )
  if (!dependants.isNullOrEmpty()) {
    data["downstream_task_ids"] = dependants.sorted()
  }
  return mapOf("__type" to "operator", "__var" to data)
}

// ---------------------------------------------------------------------------
// Task group serialization (flat root group from task list)
// ---------------------------------------------------------------------------

private fun serializeTaskGroup(taskIds: Collection<String>): Serialized =
  mapOf(
    "_group_id" to null,
    "group_display_name" to "",
    "prefix_group_id" to true,
    "tooltip" to "",
    "ui_color" to "CornflowerBlue",
    "ui_fgcolor" to "#000",
    "children" to taskIds.associateWith { listOf("operator", it) },
    "upstream_group_ids" to emptyList<String>(),
    "downstream_group_ids" to emptyList<String>(),
    "upstream_task_ids" to emptyList<String>(),
    "downstream_task_ids" to emptyList<String>(),
  )

// ---------------------------------------------------------------------------
// Params serialization
// ---------------------------------------------------------------------------

private fun serializeParams(params: Map<String, Any>): List<List<Any?>> =
  params.entries.map { (k, v) ->
    listOf(
      k,
      mapOf(
        "__class" to "airflow.sdk.definitions.param.Param",
        "default" to serializeValue(v),
        "description" to null,
        "schema" to serializeValue(emptyMap<String, Any>()),
        "source" to null,
      ),
    )
  }

// ---------------------------------------------------------------------------
// DAG serialization — matches Python's DagSerialization.serialize_dag
// ---------------------------------------------------------------------------

private fun Dag.serialize(
  id: String,
  fileloc: String,
  relativeFileloc: String,
): Serialized {
  val result =
    mutableMapOf<String, Any?>(
      // Required fields (always present)
      "dag_id" to id,
      "fileloc" to fileloc,
      "relative_fileloc" to relativeFileloc,
      // Always serialized
      "timezone" to "UTC",
      "timetable" to serializeTimetable(schedule),
      "tasks" to tasks.entries.map { (taskId, task) -> task.serialize(taskId, dependants[taskId]) },
      "dag_dependencies" to emptyList<Any>(),
      "task_group" to serializeTaskGroup(tasks.keys),
      "edge_info" to emptyMap<String, Any>(),
      "params" to (params?.let { serializeParams(it) } ?: emptyList<Any>()),
      "deadline" to null,
      "allowed_run_types" to null,
    )

  // Optional fields — only include if non-null.
  // Non-decorated fields are serialized then unwrapped (matching Python's serialize_to_json).
  description?.let { result["description"] = it }
  startDate?.let { result["start_date"] = unwrapTypeEncoding(serializeValue(it)) }
  endDate?.let { result["end_date"] = unwrapTypeEncoding(serializeValue(it)) }
  dagrunTimeout?.let { result["dagrun_timeout"] = unwrapTypeEncoding(serializeValue(it)) }
  docMd?.let { result["doc_md"] = it }
  isPausedUponCreation?.let { result["is_paused_upon_creation"] = it }

  // Decorated fields (kept with __type/__var encoding, NOT unwrapped)
  if (defaultArgs.isNotEmpty()) {
    result["default_args"] = serializeValue(defaultArgs)
  }
  if (accessControl != null) {
    // access_control is always serialized when not null, even if empty
    result["access_control"] = serializeValue(accessControl)
  }

  // Fields excluded when matching schema defaults
  if (catchup) result["catchup"] = true
  if (failFast) result["fail_fast"] = true
  if (maxActiveTasks != Dag.DEFAULT_MAX_ACTIVE_TASKS) result["max_active_tasks"] = maxActiveTasks
  if (maxActiveRuns != Dag.DEFAULT_MAX_ACTIVE_RUNS) result["max_active_runs"] = maxActiveRuns
  if (maxConsecutiveFailedDagRuns != Dag.DEFAULT_MAX_CONSECUTIVE_FAILED_DAG_RUNS) {
    result["max_consecutive_failed_dag_runs"] = maxConsecutiveFailedDagRuns
  }
  if (renderTemplateAsNativeObj) result["render_template_as_native_obj"] = true

  // dag_display_name — excluded when it equals dag_id (the default)
  if (dagDisplayName != null && dagDisplayName != id) {
    result["dag_display_name"] = dagDisplayName
  }

  // Collection fields — serialized then unwrapped; excluded when empty
  if (tags.isNotEmpty()) result["tags"] = unwrapTypeEncoding(serializeValue(tags))
  if (ownerLinks.isNotEmpty()) result["owner_links"] = unwrapTypeEncoding(serializeValue(ownerLinks))

  return result
}

/** Serialize a single DAG to a dict. Exposed for cross-language validation testing. */
internal fun serializeDag(dag: Dag): Serialized = dag.serialize(dag.dagId, "", "")

// ---------------------------------------------------------------------------
// Top-level envelope — matches Python's DagSerialization.to_dict
// ---------------------------------------------------------------------------

private fun computeRelativeFileloc(
  fileloc: String,
  bundlePath: String,
): String {
  if (fileloc.isEmpty()) return ""
  if (bundlePath.isEmpty()) return "."
  val rel = Path.of(bundlePath).relativize(Path.of(fileloc)).toString()
  return rel.ifEmpty { "." }
}

internal fun DagParsingResult.serialize(): Serialized {
  val relativeFileloc = computeRelativeFileloc(fileloc, bundlePath)
  val result =
    mapOf(
      "type" to "DagFileParsingResult",
      "fileloc" to fileloc,
      "serialized_dags" to
        dags.entries.map { (id, d) ->
          mapOf("data" to mapOf("__version" to 3, "dag" to d.serialize(id, fileloc, relativeFileloc)))
        },
    )

  logger.debug("Serialized DAG parsing result", mapOf("result" to result))

  return result
}
