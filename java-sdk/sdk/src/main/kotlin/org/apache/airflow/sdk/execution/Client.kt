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

import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.api.model.ConnectionResponse
import org.apache.airflow.sdk.execution.api.model.VariableResponse
import org.apache.airflow.sdk.execution.api.model.XComResponse

/**
 * @suppress
 *
 * Transport contract between [org.apache.airflow.sdk.Client] and the coordinator.
 *
 * Implementations translate each SDK method call into the appropriate
 * message and unwrap the raw response model into the value expected by the public
 * SDK layer.
 *
 * Currently, the only production implementation is [CoordinatorClient]. A test
 * double can be supplied via the internal [org.apache.airflow.sdk.Client]
 * constructor to exercise task logic without a live coordinator.
 */
interface Client {
  fun getConnection(id: String): ConnectionResponse

  fun getVariable(key: String): VariableResponse

  fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): XComResponse

  fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  )
}

/**
 * @suppress
 *
 * Production [Client] implementation backed by a live comm.
 *
 * Each method serializes the request into the appropriate message type (e.g.
 * [GetConnection], [GetXCom]), sends it over the comm, and returns the
 * unwrapped response model. All calls block the calling thread because task
 * [execute][org.apache.airflow.sdk.Task.execute] runs on a plain thread, not
 * inside a coroutine.
 */
class CoordinatorClient(
  val exec: CoordinatorComm,
) : Client {
  override fun getConnection(id: String) = runBlocking { exec.communicate<ConnectionResponse>(GetConnection(id)) }

  override fun getVariable(key: String) = runBlocking { exec.communicate<VariableResponse>(GetVariable(key)) }

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    val message =
      SetXCom(
        key = key,
        value = value,
        dagId = dagId,
        taskId = taskId,
        runId = runId,
        mapIndex = mapIndex,
      )
    runBlocking { exec.communicate<Unit>(message) }
  }

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResponse {
    val message =
      GetXCom(
        key = key,
        dagId = dagId,
        taskId = taskId,
        runId = runId,
        mapIndex = mapIndex,
        includePriorDates = includePriorDates,
      )
    return runBlocking { exec.communicate<XComResponse>(message) }
  }
}
