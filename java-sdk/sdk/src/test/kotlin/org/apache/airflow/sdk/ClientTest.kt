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

private data class XComCall(
  val key: String,
  val dagId: String,
  val taskId: String,
  val runId: String,
  val mapIndex: Int?,
  val includePriorDates: Boolean,
)

private class XComStats {
  @JvmField
  var rowCount: Long = 0
}

private class FakeTransport(
  val connection: ConnectionResult = ConnectionResult(),
  val stored: TaskStateStoreResult? = null,
  val xcom: Any? = null,
) : org.apache.airflow.sdk.execution.Client {
  val calls = mutableListOf<StateStoreCall>()
  val xcomCalls = mutableListOf<XComCall>()

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
  ): XComResult {
    xcomCalls.add(XComCall(key, dagId, taskId, runId, mapIndex, includePriorDates))
    return XComResult().also { it.value = xcom }
  }

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

  override fun skipDownstreamTasks(taskIds: List<String>) = Unit

  override fun triggerDagRun(
    dagId: String,
    runId: String,
    logicalDate: OffsetDateTime?,
    runAfter: OffsetDateTime?,
    conf: Map<String, Any?>?,
    resetDagRun: Boolean,
    note: String?,
  ): Boolean = throw NotImplementedError()

  override fun getDagRunState(
    dagId: String,
    runId: String,
  ): String = throw NotImplementedError()

  override fun isDagPaused(dagId: String): Boolean = throw NotImplementedError()
}

class ClientTest {
  private fun clientWith(connection: ConnectionResult) = Client(StartupDetails(), FakeTransport(connection))

  private val tiId: UUID = UUID.randomUUID()

  private fun startupDetails() = StartupDetails().also { it.ti = TaskInstance().also { ti -> ti.id = tiId } }

  private fun stateStoreClient(
    stored: TaskStateStoreResult? = null,
    env: Map<String, String> = emptyMap(),
  ): Pair<Client, FakeTransport> {
    val transport = FakeTransport(stored = stored)
    return Client(startupDetails(), transport) { env[it] } to transport
  }

  private fun assertExpiresAbout(
    expected: Duration,
    before: OffsetDateTime,
    after: OffsetDateTime,
    expiresAt: OffsetDateTime?,
  ) {
    Assertions.assertNotNull(expiresAt)
    Assertions.assertEquals(ZoneOffset.UTC, expiresAt!!.offset)
    Assertions.assertFalse(expiresAt.isBefore(before.plus(expected)), "expiresAt $expiresAt before $before + $expected")
    Assertions.assertFalse(expiresAt.isAfter(after.plus(expected)), "expiresAt $expiresAt after $after + $expected")
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

  private fun xcomClient(xcom: Any?): Pair<Client, FakeTransport> {
    val details =
      StartupDetails().also {
        it.ti =
          TaskInstance().also { ti ->
            ti.dagId = "current_dag"
            ti.runId = "current_run"
          }
      }
    val transport = FakeTransport(xcom = xcom)
    return Client(details, transport) to transport
  }

  @Test
  @DisplayName("getXCom with a class reads the XCom into that class, matching keys ignoring case and underscores")
  fun getXComReadsIntoClass() {
    val (client, _) = xcomClient(mapOf("row_count" to 5L))

    Assertions.assertEquals(5L, client.getXCom(taskId = "extract", type = XComStats::class.java)?.rowCount)
  }

  @Test
  @DisplayName("getXCom with a TypeRef keeps the element type of a generic type")
  fun getXComReadsIntoGenericType() {
    val (client, _) = xcomClient(listOf(mapOf("row_count" to 5L)))

    val stats = client.getXCom(taskId = "extract", type = object : TypeRef<List<XComStats>>() {})

    Assertions.assertEquals(5L, stats?.single()?.rowCount)
  }

  @Test
  @DisplayName("getXCom with a type fails on a key that no field of the class declares, and names the XCom")
  fun getXComFailsOnUnknownKey() {
    val (client, _) = xcomClient(mapOf("thresh" to 0.5))

    listOf<() -> Any?>(
      { client.getXCom(taskId = "extract", type = XComStats::class.java) },
      { client.getXCom(taskId = "extract", type = object : TypeRef<XComStats>() {}) },
    ).forEach { read ->
      val error = Assertions.assertThrows(IllegalArgumentException::class.java) { read() }

      val message = error.message.orEmpty()
      Assertions.assertTrue(
        message.startsWith("Cannot convert the XCom value to org.apache.airflow.sdk.XComStats: Unrecognized field"),
      ) { message }
    }
  }

  @Test
  @DisplayName("getXCom with a type returns null when nothing was pushed, even for a primitive class")
  fun getXComReturnsNullWhenNothingWasPushed() {
    val (client, _) = xcomClient(null)

    Assertions.assertNull(client.getXCom(taskId = "extract", type = XComStats::class.java))
    Assertions.assertNull(client.getXCom(taskId = "extract", type = Long::class.java))
    Assertions.assertNull(client.getXCom(taskId = "extract", type = object : TypeRef<List<XComStats>>() {}))
  }

  @Test
  @DisplayName("getXCom accepts a primitive class, which is what Long::class.java is in Kotlin")
  fun getXComAcceptsPrimitiveClass() {
    val (client, _) = xcomClient(5L)

    Assertions.assertEquals(5L, client.getXCom(taskId = "extract", type = Long::class.java))
    Assertions.assertEquals(5, client.getXCom(taskId = "extract", type = Int::class.java))
    Assertions.assertEquals(5L, client.getXCom(taskId = "extract", type = Long::class.javaObjectType))
  }

  @Test
  @DisplayName("getXCom with a type passes every argument to the transport")
  fun getXComWithTypePassesEveryArgument() {
    val (client, transport) = xcomClient(5L)
    val expected = XComCall("k", "other_dag", "extract", "other_run", 2, true)

    client.getXCom("k", "other_dag", "extract", "other_run", 2, true, Long::class.javaObjectType)
    client.getXCom("k", "other_dag", "extract", "other_run", 2, true, object : TypeRef<Long>() {})

    Assertions.assertEquals(listOf(expected, expected), transport.xcomCalls)
  }

  @Test
  @DisplayName("getXCom with a type gives Java overloads whose defaults read the return value of the current run")
  fun getXComWithTypeGivesJavaOverloads() {
    val string = String::class.java
    val boxedInt = Int::class.javaObjectType
    val primitiveBoolean = Boolean::class.java
    val classType: Any = Long::class.javaObjectType
    val typeRef: Any = object : TypeRef<Long>() {}
    val call = { key: String, dagId: String, runId: String, mapIndex: Int?, prior: Boolean ->
      XComCall(key, dagId, "extract", runId, mapIndex, prior)
    }

    listOf(Class::class.java to classType, TypeRef::class.java to typeRef).forEach { (parameter, type) ->
      val (client, transport) = xcomClient(5L)
      val overloads =
        listOf(
          arrayOf(string, parameter) to arrayOf("extract", type),
          arrayOf(string, string, parameter) to arrayOf("k", "extract", type),
          arrayOf(string, string, string, parameter) to arrayOf("k", "d", "extract", type),
          arrayOf(string, string, string, string, parameter) to arrayOf("k", "d", "extract", "r", type),
          arrayOf(string, string, string, string, boxedInt, parameter) to arrayOf("k", "d", "extract", "r", 2, type),
          arrayOf(string, string, string, string, boxedInt, primitiveBoolean, parameter) to
            arrayOf("k", "d", "extract", "r", 2, true, type),
        )

      overloads.forEach { (parameters, arguments) ->
        Client::class.java.getMethod("getXCom", *parameters).invoke(client, *arguments)
      }

      Assertions.assertEquals(
        listOf(
          call("return_value", "current_dag", "current_run", null, false),
          call("k", "current_dag", "current_run", null, false),
          call("k", "d", "current_run", null, false),
          call("k", "d", "r", null, false),
          call("k", "d", "r", 2, false),
          call("k", "d", "r", 2, true),
        ),
        transport.xcomCalls,
      )
    }
  }

  @Test
  @DisplayName("taskStateStore is exposed to Java as a getter so mocking frameworks can stub it")
  fun taskStateStoreIsExposedAsGetter() {
    val getter = Client::class.java.getMethod("getTaskStateStore")

    Assertions.assertEquals(TaskStateStore::class.java, getter.returnType)
    Assertions.assertTrue(Client::class.java.fields.none { it.name == "taskStateStore" })
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
  @DisplayName("taskStateStore.set without retention uses AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS")
  fun taskStateStoreSetWithoutRetentionUsesDefaultRetentionDays() {
    listOf("7" to 7L, "7.0" to 7L, " 2 " to 2L).forEach { (raw, days) ->
      val (client, transport) = stateStoreClient(env = mapOf(TaskStateStore.DEFAULT_RETENTION_DAYS_ENV to raw))

      val before = OffsetDateTime.now(ZoneOffset.UTC)
      client.taskStateStore.set("job_id", 42)
      val after = OffsetDateTime.now(ZoneOffset.UTC)

      assertExpiresAbout(Duration.ofDays(days), before, after, transport.calls.single().expiresAt)
    }
  }

  @Test
  @DisplayName("taskStateStore.set without retention fails when the coordinator did not pass the default")
  fun taskStateStoreSetWithoutRetentionFailsWhenVariableAbsent() {
    val (client, transport) = stateStoreClient()

    val error = Assertions.assertThrows(IllegalStateException::class.java) { client.taskStateStore.set("job_id", 42) }

    Assertions.assertTrue(error.message!!.startsWith(TaskStateStore.DEFAULT_RETENTION_DAYS_ENV), error.message)
    Assertions.assertTrue(transport.calls.isEmpty(), "no call expected: ${transport.calls}")
  }

  @Test
  @DisplayName("taskStateStore.set never expires for a default of 0 days or an explicit NEVER_EXPIRE")
  fun taskStateStoreSetNeverExpires() {
    val (byConfig, configTransport) = stateStoreClient(env = mapOf(TaskStateStore.DEFAULT_RETENTION_DAYS_ENV to "0"))
    val (byArgument, argumentTransport) = stateStoreClient()

    byConfig.taskStateStore.set("job_id", 42)
    byArgument.taskStateStore.set("job_id", 42, TaskStateStore.NEVER_EXPIRE)

    Assertions.assertEquals(listOf(StateStoreCall("set", tiId, "job_id", 42, expiresAt = null)), configTransport.calls)
    Assertions.assertEquals(listOf(StateStoreCall("set", tiId, "job_id", 42, expiresAt = null)), argumentTransport.calls)
  }

  @Test
  @DisplayName("taskStateStore.set with retention expires that long after now, in UTC")
  fun taskStateStoreSetWithRetentionComputesExpiry() {
    val (client, transport) = stateStoreClient()
    val retention = Duration.ofHours(6)

    val before = OffsetDateTime.now(ZoneOffset.UTC)
    client.taskStateStore.set("job_id", 42, retention)
    val after = OffsetDateTime.now(ZoneOffset.UTC)

    assertExpiresAbout(retention, before, after, transport.calls.single().expiresAt)
  }

  @Test
  @DisplayName("taskStateStore.set rejects a zero or negative retention without calling the supervisor")
  fun taskStateStoreSetRejectsNonPositiveRetention() {
    listOf(Duration.ZERO, Duration.ofSeconds(-1)).forEach { retention ->
      val (client, transport) = stateStoreClient()

      Assertions.assertThrows(IllegalArgumentException::class.java) { client.taskStateStore.set("job_id", 42, retention) }
      Assertions.assertTrue(transport.calls.isEmpty(), "no call expected for $retention: ${transport.calls}")
    }
  }

  @Test
  @DisplayName("taskStateStore.set rejects a default retention that is not a non-negative integer")
  fun taskStateStoreSetRejectsBadDefaultRetention() {
    listOf("abc", "-1", "1.5", "").forEach { raw ->
      val (client, transport) = stateStoreClient(env = mapOf(TaskStateStore.DEFAULT_RETENTION_DAYS_ENV to raw))

      Assertions.assertThrows(IllegalArgumentException::class.java) { client.taskStateStore.set("job_id", 42) }
      Assertions.assertTrue(transport.calls.isEmpty(), "no call expected for '$raw': ${transport.calls}")
    }
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
