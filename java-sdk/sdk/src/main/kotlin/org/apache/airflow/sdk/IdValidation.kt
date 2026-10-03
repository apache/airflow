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

package org.apache.airflow.sdk

/**
 * Best-effort checks of Dag and task IDs against the rules the Airflow server
 * enforces (`airflow.utils.helpers.validate_key`). Warn-only: the server
 * validates authoritatively, and checks like the `..` one depend on server
 * configuration this process cannot see. [BundleInspector] reports the
 * findings at build time.
 */
internal object IdValidation {
  private const val MAX_ID_LENGTH = 250
  private val ID_REGEX = Regex("""^[\p{L}\p{N}_.-]+$""")

  fun findSuspiciousIds(dags: Iterable<DagDef>): List<IdWarning> {
    val warnings = mutableListOf<IdWarning>()

    fun check(
      label: String,
      id: String,
      arguments: Map<String, Any>,
    ) {
      val length = id.codePointCount(0, id.length)
      if (length > MAX_ID_LENGTH) {
        warnings +=
          IdWarning(
            "$label is longer than $MAX_ID_LENGTH characters; the Airflow server will reject it",
            arguments + ("length" to length),
          )
      }
      if (!ID_REGEX.matches(id)) {
        warnings +=
          IdWarning(
            "$label must be made of alphanumeric characters, dashes, dots, and underscores; " +
              "the Airflow server will reject it",
            arguments,
          )
      } else if (id.contains("..")) {
        warnings +=
          IdWarning(
            "$label contains '..'; the Airflow server will reject it " +
              "unless [core] allow_double_dot_in_ids is enabled",
            arguments,
          )
      }
    }

    for (dag in dags) {
      check("Dag id", dag.id, mapOf("dag_id" to dag.id))
      for (taskId in dag.tasks.keys) {
        check("Task id", taskId, mapOf("dag_id" to dag.id, "task_id" to taskId))
      }
    }
    return warnings
  }
}

internal data class IdWarning(
  val message: String,
  val arguments: Map<String, Any>,
) {
  fun render(): String =
    "$message (${
      arguments.entries.joinToString(", ") { (key, value) ->
        if (value is String) "$key=\"$value\"" else "$key=$value"
      }
    })"
}
