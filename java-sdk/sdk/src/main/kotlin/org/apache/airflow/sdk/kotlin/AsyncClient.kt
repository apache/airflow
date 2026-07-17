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

package org.apache.airflow.sdk.kotlin

import org.apache.airflow.sdk.Connection
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.toSdkConnection
import org.apache.airflow.sdk.execution.AsyncClient as TransportClient

/**
 * Coroutine-native client for Airflow API calls scoped to the current task
 * instance.
 *
 * An instance is provided to [AsyncTask.execute]. All operations suspend their
 * coroutine while waiting for the coordinator and can participate in structured
 * concurrency.
 */
class AsyncClient internal constructor(
  internal val details: StartupDetails,
  internal val impl: TransportClient,
) {
  internal companion object {
    const val XCOM_RETURN_KEY = "return_value"
  }

  /** Retrieves a connection from the Airflow connection store. */
  suspend fun getConnection(id: String): Connection = impl.getConnection(id).toSdkConnection()

  /** Retrieves an Airflow variable, or `null` if it is not set. */
  suspend fun getVariable(key: String): Any? = impl.getVariable(key).value

  /**
   * Reads an XCom value pushed by another task.
   *
   * The current Dag run and task-instance context provide the default Dag and
   * run identifiers.
   */
  suspend fun getXCom(
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

  /** Pushes an XCom value for downstream tasks to read. */
  suspend fun setXCom(
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
