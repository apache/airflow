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

import org.apache.airflow.sdk.execution.Client
import org.apache.airflow.sdk.execution.comm.StartupDetails

/**
 * A connection registered in Airflow's connection store.
 *
 * @property id Connection ID as configured in Airflow.
 * @property type Connection type (e.g. `"http"`, `"postgres"`), if configured.
 * @property host Hostname, if configured.
 * @property schema Schema or database name, if configured.
 * @property login Username, if configured.
 * @property password Password, if configured.
 * @property port Port number, if configured.
 * @property extra JSON blob of extra connection parameters, if configured.
 */
data class Connection(
  @JvmField val id: String,
  @JvmField val type: String?,
  @JvmField val host: String?,
  @JvmField val schema: String?,
  @JvmField val login: String?,
  @JvmField val password: String?,
  @JvmField val port: Int?,
  @JvmField val extra: Any?,
)

/**
 * Client for Airflow API calls scoped to the current task instance.
 *
 * An instance is provided when a task is being executed. All reads and writes
 * are automatically scoped to the current Dag run and task instance unless you
 * pass explicit IDs.
 */
class Client internal constructor(
  internal val details: StartupDetails,
  internal val impl: Client,
) {
  internal companion object {
    /**
     * Default XCom key used for a task's return value ({@value}).
     */
    const val XCOM_RETURN_KEY = "return_value"
  }

  /**
   * Retrieves a connection from the Airflow connection store.
   *
   * @param id Connection ID as configured in Airflow.
   * @return The connection.
   * @throws ApiError if the connection does not exist or the API call fails.
   */
  fun getConnection(id: String): Connection =
    with(impl.getConnection(id)) {
      Connection(
        id = connId,
        type = connType,
        host = host as String?,
        schema = schema as String?,
        login = login as String?,
        password = password as String?,
        // The msgpack decoder yields Long for wire integers, so convert
        // numerically instead of casting.
        port = (port as Number?)?.toInt(),
        extra = extra,
      )
    }

  /**
   * Retrieves an Airflow variable.
   *
   * @param key Variable key.
   * @return The variable value, or `null` if the variable is not set.
   * @throws ApiError if the API call fails.
   */
  fun getVariable(key: String): Any? = impl.getVariable(key).value

  /**
   * Reads an XCom value pushed by another task.
   *
   * The current Dag run's [dagId][TaskInstance.dagId] and
   * [runId][TaskInstance.runId] are used by default; override them only when
   * reading across Dags or runs.
   *
   * @param key XCom key to read; defaults to [XCOM_RETURN_KEY].
   * @param dagId Dag that owns the XCom; defaults to the current Dag.
   * @param taskId Task that pushed the XCom.
   * @param runId Run that produced the XCom; defaults to the current run.
   * @param mapIndex Map index of the source task instance.
   * @param includePriorDates If `true`, also search earlier Dag-run dates.
   * @return The XCom value, or `null` if none was pushed.
   * @throws ApiError if the API call fails.
   *
   * If `map_index` is set to `null` against a mapped task, the task's
   * "collective result" is returned. Results from all mapped instances are
   * aggregated into a list, ordered by the map index (ascending). For a
   * non-mapped task, setting `map_index` to `null` is equivalent to `-1`.
   */
  @JvmOverloads fun getXCom(
    key: String = XCOM_RETURN_KEY,
    dagId: String = details.ti.dagId,
    taskId: String,
    runId: String = details.ti.runId,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): Any? =
    impl
      .getXCom(
        key = key,
        dagId = dagId,
        taskId = taskId,
        runId = runId,
        mapIndex = mapIndex,
        includePriorDates = includePriorDates,
      ).value

  /**
   * Pushes an XCom value for downstream tasks to read.
   *
   * @param key XCom key; defaults to [XCOM_RETURN_KEY].
   * @param value Value to push. Must be JSON-serializable.
   * @throws ApiError if the API call fails.
   */
  @JvmOverloads fun setXCom(
    key: String = XCOM_RETURN_KEY,
    value: Any,
  ) = impl.setXCom(
    key = key,
    value = value,
    dagId = details.ti.dagId,
    taskId = details.ti.taskId,
    runId = details.ti.runId,
    mapIndex = details.ti.mapIndex ?: -1,
  )
}
