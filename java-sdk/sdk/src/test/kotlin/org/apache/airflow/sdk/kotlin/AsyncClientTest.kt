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

import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

private class FakeAsyncTransport : org.apache.airflow.sdk.execution.AsyncClient {
  val setXComCalls = mutableListOf<List<Any?>>()

  override suspend fun getConnection(id: String) =
    ConnectionResult().apply {
      connId = id
      connType = "http"
      port = 8080L
    }

  override suspend fun getVariable(key: String) = VariableResult().apply { value = "value:$key" }

  override suspend fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ) = XComResult().apply { value = listOf(key, dagId, taskId, runId, mapIndex, includePriorDates) }

  override suspend fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    setXComCalls += listOf(key, value, dagId, taskId, runId, mapIndex)
  }
}

class AsyncClientTest {
  @Test
  fun shouldForwardOperationsWithTaskContext() =
    runBlocking {
      val transport = FakeAsyncTransport()
      val details =
        StartupDetails().apply {
          ti =
            TaskInstance().apply {
              dagId = "dag"
              taskId = "task"
              runId = "run"
              mapIndex = 3
            }
        }
      val client = AsyncClient(details, transport)

      val connection = client.getConnection("http")
      val variable = client.getVariable("key")
      val xcom = client.getXCom(taskId = "upstream")
      client.setXCom(value = 42)
      val explicitXCom =
        client.getXCom(
          key = "custom",
          dagId = "other-dag",
          taskId = "other-task",
          runId = "other-run",
          mapIndex = 7,
          includePriorDates = true,
        )
      details.ti.mapIndex = null
      client.setXCom(key = "custom", value = 43)

      Assertions.assertEquals("http", connection.id)
      Assertions.assertEquals(8080, connection.port)
      Assertions.assertEquals("value:key", variable)
      Assertions.assertEquals(listOf("return_value", "dag", "upstream", "run", null, false), xcom)
      Assertions.assertEquals(listOf("custom", "other-dag", "other-task", "other-run", 7, true), explicitXCom)
      Assertions.assertEquals(
        listOf(
          listOf("return_value", 42, "dag", "task", "run", 3),
          listOf("custom", 43, "dag", "task", "run", -1),
        ),
        transport.setXComCalls,
      )
    }
}
