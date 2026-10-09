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
import org.apache.airflow.sdk.execution.comm.ClearTaskStateStore
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.DagResult
import org.apache.airflow.sdk.execution.comm.DagRunStateResult
import org.apache.airflow.sdk.execution.comm.DeleteTaskStateStore
import org.apache.airflow.sdk.execution.comm.DeleteVariable
import org.apache.airflow.sdk.execution.comm.DeleteXCom
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import org.apache.airflow.sdk.execution.comm.GetConnection
import org.apache.airflow.sdk.execution.comm.GetDag
import org.apache.airflow.sdk.execution.comm.GetDagRunState
import org.apache.airflow.sdk.execution.comm.GetTaskStateStore
import org.apache.airflow.sdk.execution.comm.GetVariable
import org.apache.airflow.sdk.execution.comm.GetXCom
import org.apache.airflow.sdk.execution.comm.OKResponse
import org.apache.airflow.sdk.execution.comm.PutVariable
import org.apache.airflow.sdk.execution.comm.SetTaskStateStore
import org.apache.airflow.sdk.execution.comm.SetXCom
import org.apache.airflow.sdk.execution.comm.SkipDownstreamTasks
import org.apache.airflow.sdk.execution.comm.TaskStateStoreResult
import org.apache.airflow.sdk.execution.comm.TriggerDagRun
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @suppress
 *
 * Transport contract between [org.apache.airflow.sdk.Client] and the coordinator.
 *
 * Implementations translate each SDK method call into the appropriate message
 * and unwrap the raw response model into the value expected by the public SDK
 * layer.
 *
 * Currently, the only production implementation is [CoordinatorClient]. A test
 * double can be supplied via the internal [org.apache.airflow.sdk.Client]
 * constructor to exercise task logic without a live coordinator.
 */
interface Client {
  fun getConnection(id: String): ConnectionResult

  fun getVariable(key: String): VariableResult

  fun setVariable(
    key: String,
    value: String,
    description: String?,
  )

  fun deleteVariable(key: String)

  fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): XComResult

  fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  )

  fun deleteXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  )

  /** Returns `null` when the key is not stored for the task instance. */
  fun getTaskStateStore(
    tiId: UUID,
    key: String,
  ): TaskStateStoreResult?

  fun setTaskStateStore(
    tiId: UUID,
    key: String,
    value: Any,
    expiresAt: OffsetDateTime?,
  )

  fun deleteTaskStateStore(
    tiId: UUID,
    key: String,
  )

  fun clearTaskStateStore(tiId: UUID)

  /**
   * Marks [taskIds] of the running Dag run skipped, as a decider's untaken
   * branches are. Airflow leaves a task instance that is already running,
   * succeeded or failed alone.
   */
  fun skipDownstreamTasks(taskIds: List<String>)

  /**
   * Starts a run of another Dag, as a task declared from a
   * [org.apache.airflow.sdk.TriggerDagRun] does.
   *
   * @return True when a run with that ID already exists, which the task
   *    decides what to do about, rather than an error.
   */
  fun triggerDagRun(
    dagId: String,
    runId: String,
    logicalDate: OffsetDateTime?,
    runAfter: OffsetDateTime?,
    conf: Map<String, Any?>?,
    resetDagRun: Boolean,
    note: String?,
  ): Boolean

  /** State of one Dag run, which a task that waits polls. */
  fun getDagRunState(
    dagId: String,
    runId: String,
  ): String

  /** Whether the Dag is paused, which a task may refuse to trigger. */
  fun isDagPaused(dagId: String): Boolean
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
  override fun getConnection(id: String) =
    runBlocking {
      exec.communicate<ConnectionResult>(GetConnection().apply { connId = id })
    }

  override fun getVariable(key: String) =
    runBlocking {
      exec.communicate<VariableResult>(GetVariable().also { it.key = key })
    }

  override fun setVariable(
    key: String,
    value: String,
    description: String?,
  ) {
    val message =
      PutVariable().also {
        it.key = key
        it.value = value
        it.description = description
      }
    runBlocking { exec.communicate<Unit>(message) }
  }

  override fun deleteVariable(key: String) {
    runBlocking { exec.communicate<OKResponse>(DeleteVariable().also { it.key = key }) }
  }

  override fun setXCom(
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
    runBlocking { exec.communicate<Unit>(message) }
  }

  override fun getXCom(
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
    return runBlocking { exec.communicate<XComResult>(message) }
  }

  override fun deleteXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    val message =
      DeleteXCom().also {
        it.key = key
        it.dagId = dagId
        it.taskId = taskId
        it.runId = runId
        it.mapIndex = mapIndex
      }
    runBlocking { exec.communicate<Unit>(message) }
  }

  override fun getTaskStateStore(
    tiId: UUID,
    key: String,
  ): TaskStateStoreResult? {
    val message =
      GetTaskStateStore().also {
        it.tiId = tiId
        it.key = key
      }
    return runBlocking {
      exec.communicateOrNullIf<TaskStateStoreResult>(message, ErrorResponse.ErrorType.TASK_STORE_NOT_FOUND)
    }
  }

  override fun setTaskStateStore(
    tiId: UUID,
    key: String,
    value: Any,
    expiresAt: OffsetDateTime?,
  ) {
    val message =
      SetTaskStateStore().also {
        it.tiId = tiId
        it.key = key
        it.value = value
        it.expiresAt = expiresAt
      }
    runBlocking { exec.communicate<OKResponse>(message) }
  }

  override fun deleteTaskStateStore(
    tiId: UUID,
    key: String,
  ) {
    val message =
      DeleteTaskStateStore().also {
        it.tiId = tiId
        it.key = key
      }
    runBlocking { exec.communicate<OKResponse>(message) }
  }

  override fun clearTaskStateStore(tiId: UUID) {
    runBlocking { exec.communicate<OKResponse>(ClearTaskStateStore().also { it.tiId = tiId }) }
  }

  override fun skipDownstreamTasks(taskIds: List<String>) {
    if (taskIds.isEmpty()) return
    val message = SkipDownstreamTasks().also { it.tasks = ArrayList<Any>(taskIds) }
    runBlocking { exec.communicate<Unit>(message) }
  }

  override fun triggerDagRun(
    dagId: String,
    runId: String,
    logicalDate: OffsetDateTime?,
    runAfter: OffsetDateTime?,
    conf: Map<String, Any?>?,
    resetDagRun: Boolean,
    note: String?,
  ): Boolean {
    val message =
      TriggerDagRun().also {
        it.dagId = dagId
        it.runId = runId
        it.logicalDate = logicalDate
        it.runAfter = runAfter
        it.conf = conf
        it.resetDagRun = resetDagRun
        it.note = note
      }
    // The supervisor answers a trigger with an OKResponse, and reports a run that already exists as
    // the DAGRUN_ALREADY_EXISTS error, which the task decides what to do about.
    val response =
      runBlocking {
        exec.communicateOrNullIf<OKResponse>(message, ErrorResponse.ErrorType.DAGRUN_ALREADY_EXISTS)
      }
    return response == null
  }

  override fun getDagRunState(
    dagId: String,
    runId: String,
  ): String {
    val message =
      GetDagRunState().also {
        it.dagId = dagId
        it.runId = runId
      }
    return runBlocking { exec.communicate<DagRunStateResult>(message) }.state.toString()
  }

  override fun isDagPaused(dagId: String): Boolean =
    runBlocking { exec.communicate<DagResult>(GetDag().also { it.dagId = dagId }) }.isPaused == true
}
