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

import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.GetConnection
import org.apache.airflow.sdk.execution.comm.GetVariable
import org.apache.airflow.sdk.execution.comm.GetXCom
import org.apache.airflow.sdk.execution.comm.SetXCom
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult

/** @suppress */
interface AsyncClient {
  suspend fun getConnection(id: String): ConnectionResult

  suspend fun getVariable(key: String): VariableResult

  suspend fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): XComResult

  suspend fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  )
}

/** @suppress */
class CoordinatorAsyncClient(
  val exec: CoordinatorComm,
) : AsyncClient {
  override suspend fun getConnection(id: String) = exec.communicate<ConnectionResult>(GetConnection().apply { connId = id })

  override suspend fun getVariable(key: String) = exec.communicate<VariableResult>(GetVariable().also { it.key = key })

  override suspend fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    val message =
      SetXCom().also {
        it.key = key
        it.value = value
        it.dagId = dagId
        it.taskId = taskId
        it.runId = runId
        it.mapIndex = mapIndex
      }
    exec.communicate<Unit>(message)
  }

  override suspend fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResult {
    val message =
      GetXCom().also {
        it.key = key
        it.dagId = dagId
        it.taskId = taskId
        it.runId = runId
        it.mapIndex = mapIndex
        it.includePriorDates = includePriorDates
      }
    return exec.communicate<XComResult>(message)
  }
}
