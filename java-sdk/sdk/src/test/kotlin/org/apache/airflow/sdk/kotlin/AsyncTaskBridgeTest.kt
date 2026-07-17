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

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume

private class BridgeTransport : org.apache.airflow.sdk.execution.AsyncClient {
  val published = mutableListOf<Any>()

  override suspend fun getConnection(id: String): ConnectionResult = throw UnsupportedOperationException()

  override suspend fun getVariable(key: String): VariableResult = throw UnsupportedOperationException()

  override suspend fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ) = XComResult().apply { value = taskId.length }

  override suspend fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    published += value
  }
}

class AsyncTaskBridgeTest {
  @Test
  fun shouldResolveXComsAndPublishTaskResult() =
    runBlocking {
      val transport = BridgeTransport()
      val client =
        AsyncClient(
          StartupDetails().apply {
            ti =
              TaskInstance().apply {
                dagId = "dag"
                taskId = "task"
                runId = "run"
              }
          },
          transport,
        )

      suspendCoroutineUninterceptedOrReturn<Unit> { completion ->
        AsyncTaskBridge.execute<Int>(
          client,
          arrayOf("upstream"),
          publishResult = true,
          call =
            SuspendTaskCall<Int> { values, taskCompletion ->
              val result = (values.single() as Int) + 1
              launch {
                yield()
                taskCompletion.resume(result)
              }
              COROUTINE_SUSPENDED
            },
          completion,
        )
      }

      Assertions.assertEquals(listOf(9), transport.published)
    }

  @Test
  fun shouldSkipPublishingUnitResult() =
    runBlocking {
      val transport = BridgeTransport()
      val client =
        AsyncClient(
          StartupDetails().apply { ti = TaskInstance() },
          transport,
        )

      suspendCoroutineUninterceptedOrReturn<Unit> { completion ->
        AsyncTaskBridge.execute<Unit>(
          client,
          emptyArray(),
          publishResult = false,
          call = SuspendTaskCall<Unit> { _, _ -> Unit },
          completion,
        )
      }

      Assertions.assertTrue(transport.published.isEmpty())
    }
}
