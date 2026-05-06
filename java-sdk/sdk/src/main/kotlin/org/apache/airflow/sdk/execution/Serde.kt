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
 * - Map -> {"__type": "dict", "__var": {k: serialize(v), ...}}
 * - Set -> {"__type": "set", "__var": [sorted items]}
 * - Datetime -> {"__type": "datetime", "__var": epoch_seconds}
 * - Timedelta -> {"__type": "timedelta", "__var": total_seconds}
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

// ---------------------------------------------------------------------------
// Timetable serialization
// ---------------------------------------------------------------------------

private fun serializeTimetable(): Serialized =
  mapOf(
    "__type" to "airflow.timetables.simple.NullTimetable",
    "__var" to emptyMap<String, Any>(),
  )

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
      "sdk" to "java",
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
): Serialized =
  mutableMapOf(
    // Required fields (always present)
    "dag_id" to id,
    "fileloc" to fileloc,
    "relative_fileloc" to relativeFileloc,
    // Always serialized
    "timezone" to "UTC",
    "timetable" to serializeTimetable(),
    "tasks" to tasks.entries.map { (taskId, task) -> task.serialize(taskId, dependants[taskId]) },
    "dag_dependencies" to emptyList<Any>(),
    "task_group" to serializeTaskGroup(tasks.keys),
    "edge_info" to emptyMap<String, Any>(),
    "params" to serializeParams(emptyMap()),
    "deadline" to null,
    "allowed_run_types" to null,
  )

/** Serialize a single DAG to a dict. Exposed for cross-language validation testing. */
internal fun serializeDag(dag: Dag): Serialized = dag.serialize(dag.id, "", "")

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
