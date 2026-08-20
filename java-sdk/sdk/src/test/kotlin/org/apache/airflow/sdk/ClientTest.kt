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

import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

private class FakeTransport(
  val connection: ConnectionResult,
) : org.apache.airflow.sdk.execution.Client {
  override fun getConnection(id: String): ConnectionResult = connection

  override fun getVariable(key: String): VariableResult = throw NotImplementedError()

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResult = throw NotImplementedError()

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) = throw NotImplementedError()
}

class ClientTest {
  private fun clientWith(connection: ConnectionResult) = Client(StartupDetails(), FakeTransport(connection))

  @Test
  @DisplayName("Should convert a Long port from the wire to Int")
  fun shouldConvertLongPort() {
    val result =
      ConnectionResult().apply {
        connId = "test_http"
        connType = "http"
        host = "example.com"
        // The msgpack decoder yields Long for wire integers.
        port = 8080L
      }

    val connection = clientWith(result).getConnection("test_http")

    Assertions.assertEquals("test_http", connection.id)
    Assertions.assertEquals("http", connection.type)
    Assertions.assertEquals("example.com", connection.host)
    Assertions.assertEquals(8080, connection.port)
  }

  @Test
  @DisplayName("Should keep an unset port null")
  fun shouldKeepUnsetPortNull() {
    val result =
      ConnectionResult().apply {
        connId = "test_http"
        connType = "http"
      }

    val connection = clientWith(result).getConnection("test_http")

    Assertions.assertNull(connection.port)
  }

  @Test
  @DisplayName("MissingXComException builds the full message naming the task and parameter")
  fun missingXComExceptionBuildsFullMessage() {
    val ex = MissingXComException("produce", "value")
    Assertions.assertEquals(
      "Task parameter 'value' requires an XCom from task 'produce', but none was pushed. " +
        "This parameter has a primitive type that cannot be null; declare it with a boxed type " +
        "(e.g. Integer instead of int) to receive null.",
      ex.message,
    )
  }
}
