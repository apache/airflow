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
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.TaskStateStoreResult
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private data class StateStoreCall(
  val method: String,
  val tiId: UUID,
  val key: String? = null,
  val value: Any? = null,
  val expiresAt: OffsetDateTime? = null,
)

private class FakeTransport(
  val connection: ConnectionResult = ConnectionResult(),
  val stored: TaskStateStoreResult? = null,
) : org.apache.airflow.sdk.execution.Client {
  val calls = mutableListOf<StateStoreCall>()

  override fun getConnection(id: String): ConnectionResult = connection

  override fun getVariable(key: String): VariableResult = throw NotImplementedError()

  override fun setVariable(
    key: String,
    value: String,
    description: String?,
  ) = throw NotImplementedError()

  override fun deleteVariable(key: String) = throw NotImplementedError()

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

  override fun getTaskStateStore(
    tiId: UUID,
    key: String,
  ): TaskStateStoreResult? {
    calls.add(StateStoreCall("get", tiId, key))
    return stored
  }

  override fun setTaskStateStore(
    tiId: UUID,
    key: String,
    value: Any,
    expiresAt: OffsetDateTime?,
  ) {
    calls.add(StateStoreCall("set", tiId, key, value, expiresAt))
  }

  override fun deleteTaskStateStore(
    tiId: UUID,
    key: String,
  ) {
    calls.add(StateStoreCall("delete", tiId, key))
  }

  override fun clearTaskStateStore(tiId: UUID) {
    calls.add(StateStoreCall("clear", tiId))
  }
}

class ClientTest {
  private fun clientWith(connection: ConnectionResult) = Client(StartupDetails(), FakeTransport(connection))

  private val tiId: UUID = UUID.randomUUID()

  private fun startupDetails() = StartupDetails().also { it.ti = TaskInstance().also { ti -> ti.id = tiId } }

  private fun stateStoreClient(stored: TaskStateStoreResult? = null): Pair<Client, FakeTransport> {
    val transport = FakeTransport(stored = stored)
    return Client(startupDetails(), transport) to transport
  }

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
  @DisplayName("taskStateStore.get returns the stored value scoped to the current task instance")
  fun taskStateStoreGetReturnsStoredValue() {
    val (client, transport) = stateStoreClient(TaskStateStoreResult().apply { value = "job-42" })

    Assertions.assertEquals("job-42", client.taskStateStore.get("job_id"))
    Assertions.assertEquals(listOf(StateStoreCall("get", tiId, "job_id")), transport.calls)
  }

  @Test
  @DisplayName("taskStateStore.get returns null when the key is not stored")
  fun taskStateStoreGetReturnsNullWhenMissing() {
    val (client, _) = stateStoreClient(stored = null)

    Assertions.assertNull(client.taskStateStore.get("job_id"))
  }

  @Test
  @DisplayName("taskStateStore.set without retention never expires")
  fun taskStateStoreSetWithoutRetentionNeverExpires() {
    val (client, transport) = stateStoreClient()

    client.taskStateStore.set("job_id", 42)

    Assertions.assertEquals(listOf(StateStoreCall("set", tiId, "job_id", 42, expiresAt = null)), transport.calls)
  }

  @Test
  @DisplayName("taskStateStore.set with retention expires that long after now, in UTC")
  fun taskStateStoreSetWithRetentionComputesExpiry() {
    val (client, transport) = stateStoreClient()
    val retention = Duration.ofHours(6)

    val before = OffsetDateTime.now(ZoneOffset.UTC)
    client.taskStateStore.set("job_id", 42, retention)
    val after = OffsetDateTime.now(ZoneOffset.UTC)

    val expiresAt = transport.calls.single().expiresAt
    Assertions.assertNotNull(expiresAt)
    Assertions.assertEquals(ZoneOffset.UTC, expiresAt!!.offset)
    Assertions.assertFalse(expiresAt.isBefore(before.plus(retention)), "expiresAt $expiresAt before $before + 6h")
    Assertions.assertFalse(expiresAt.isAfter(after.plus(retention)), "expiresAt $expiresAt after $after + 6h")
  }

  @Test
  @DisplayName("taskStateStore.delete and clear address the current task instance")
  fun taskStateStoreDeleteAndClearUseCurrentTaskInstance() {
    val (client, transport) = stateStoreClient()

    client.taskStateStore.delete("job_id")
    client.taskStateStore.clear()

    Assertions.assertEquals(
      listOf(StateStoreCall("delete", tiId, "job_id"), StateStoreCall("clear", tiId)),
      transport.calls,
    )
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
