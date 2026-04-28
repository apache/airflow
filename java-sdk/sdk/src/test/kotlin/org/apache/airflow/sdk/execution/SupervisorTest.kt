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
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.apache.airflow.sdk.execution.api.model.TaskInstanceState as ExecutionTaskInstanceState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

class SupervisorTest {
  // streamLines() tests

  @Test
  @DisplayName("streamLines: empty stream produces no callbacks")
  fun streamLinesEmptyStream() =
    runBlocking {
      val lines = mutableListOf<String>()
      Supervisor.streamLines(ByteArrayInputStream(ByteArray(0))) { lines.add(it) }
      assertTrue(lines.isEmpty())
    }

  @Test
  @DisplayName("streamLines: single line")
  fun streamLinesSingleLine() =
    runBlocking {
      val lines = mutableListOf<String>()
      Supervisor.streamLines(ByteArrayInputStream("hello\n".toByteArray())) { lines.add(it) }
      assertEquals(listOf("hello"), lines)
    }

  @Test
  @DisplayName("streamLines: multiple lines")
  fun streamLinesMultipleLines() =
    runBlocking {
      val input = "line1\nline2\nline3\n".toByteArray()
      val lines = mutableListOf<String>()
      Supervisor.streamLines(ByteArrayInputStream(input)) { lines.add(it) }
      assertEquals(listOf("line1", "line2", "line3"), lines)
    }

  @Test
  @DisplayName("streamLines: preserves blank lines between content")
  fun streamLinesWithBlankLines() =
    runBlocking {
      val input = "first\n\nsecond\n".toByteArray()
      val lines = mutableListOf<String>()
      Supervisor.streamLines(ByteArrayInputStream(input)) { lines.add(it) }
      assertEquals(listOf("first", "", "second"), lines)
    }

  @Test
  @DisplayName("streamLines: handles line without trailing newline")
  fun streamLinesNoTrailingNewline() =
    runBlocking {
      val lines = mutableListOf<String>()
      Supervisor.streamLines(ByteArrayInputStream("no-newline".toByteArray())) { lines.add(it) }
      assertEquals(listOf("no-newline"), lines)
    }

  @Test
  @DisplayName("streamLines: handles large number of lines")
  fun streamLinesManyLines() =
    runBlocking {
      val count = 10_000
      val input = (1..count).joinToString("\n") { "line-$it" }.toByteArray()
      val lines = CopyOnWriteArrayList<String>()
      Supervisor.streamLines(ByteArrayInputStream(input)) { lines.add(it) }
      assertEquals(count, lines.size)
      assertEquals("line-1", lines.first())
      assertEquals("line-$count", lines.last())
    }

  // Data class tests

  @Test
  @DisplayName("SupervisorTaskInstance: all fields populated")
  fun supervisorTaskInstanceAllFields() {
    val id = UUID.randomUUID()
    val dagVersionId = UUID.randomUUID()
    val carrier = mapOf("trace" to "abc")
    val ti =
      SupervisorTaskInstance(
        id = id,
        taskId = "my_task",
        dagId = "my_dag",
        runId = "run_1",
        tryNumber = 2,
        dagVersionId = dagVersionId,
        mapIndex = 5,
        contextCarrier = carrier,
      )
    assertEquals(id, ti.id)
    assertEquals("my_task", ti.taskId)
    assertEquals("my_dag", ti.dagId)
    assertEquals("run_1", ti.runId)
    assertEquals(2, ti.tryNumber)
    assertEquals(dagVersionId, ti.dagVersionId)
    assertEquals(5, ti.mapIndex)
    assertEquals(carrier, ti.contextCarrier)
  }

  @Test
  @DisplayName("SupervisorTaskInstance: null optional fields")
  fun supervisorTaskInstanceNullOptionals() {
    val ti =
      SupervisorTaskInstance(
        id = UUID.randomUUID(),
        taskId = "t",
        dagId = "d",
        runId = "r",
        tryNumber = 1,
        dagVersionId = UUID.randomUUID(),
        mapIndex = null,
      )
    assertEquals(null, ti.mapIndex)
    assertEquals(null, ti.contextCarrier)
  }

  @Test
  @DisplayName("SupervisorTaskInstance: data class equality")
  fun supervisorTaskInstanceEquality() {
    val id = UUID.randomUUID()
    val dvId = UUID.randomUUID()
    val a = SupervisorTaskInstance(id, "t", "d", "r", 1, dvId, null)
    val b = SupervisorTaskInstance(id, "t", "d", "r", 1, dvId, null)
    assertEquals(a, b)
    assertEquals(a.hashCode(), b.hashCode())
  }

  @Test
  @DisplayName("SupervisorBundleInfo: with and without version")
  fun supervisorBundleInfo() {
    val withVersion = SupervisorBundleInfo("my-bundle", "v2")
    assertEquals("my-bundle", withVersion.name)
    assertEquals("v2", withVersion.version)

    val withoutVersion = SupervisorBundleInfo("my-bundle", null)
    assertEquals(null, withoutVersion.version)
  }

  @Test
  @DisplayName("SupervisorResult: success and failure states")
  fun supervisorResult() {
    val success = SupervisorResult(ExecutionTaskInstanceState.SUCCESS, 0)
    assertEquals(ExecutionTaskInstanceState.SUCCESS, success.finalState)
    assertEquals(0, success.exitCode)

    val failure = SupervisorResult(ExecutionTaskInstanceState.FAILED, 1)
    assertEquals(ExecutionTaskInstanceState.FAILED, failure.finalState)
    assertEquals(1, failure.exitCode)
  }

  @Test
  @DisplayName("SupervisorRequest: default values")
  fun supervisorRequestDefaults() {
    val request =
      SupervisorRequest(
        mainClass = "com.example.Main",
        classpath = "/app/lib/*",
        executionApiBaseUrl = "http://localhost:8080/execution/",
        token = "test-token",
        workerName = "worker-1",
        userName = "airflow",
        dagRelPath = "dags/my_dag.jar",
        bundleInfo = SupervisorBundleInfo("bundle", "1"),
        taskInstance =
          SupervisorTaskInstance(
            UUID.randomUUID(),
            "task",
            "dag",
            "run",
            1,
            UUID.randomUUID(),
            null,
          ),
      )
    assertEquals("", request.sentryIntegration)
  }

  // Integration tests: Supervisor.run() with real subprocess + MockWebServer

  private lateinit var mockServer: MockWebServer

  @BeforeEach
  fun setUp() {
    mockServer = MockWebServer()
  }

  @AfterEach
  fun tearDown() {
    mockServer.shutdown()
  }

  /** Minimal valid JSON for TIRunContext that Jackson can deserialize with unknown-props disabled. */
  private val tiRunContextJson =
    """
    {
      "dag_run": {
        "dag_id": "test_dag",
        "run_id": "run_1",
        "logical_date": "2026-01-01T00:00:00Z",
        "data_interval_start": "2026-01-01T00:00:00Z",
        "data_interval_end": "2026-01-01T01:00:00Z",
        "start_date": "2026-01-01T00:00:00Z",
        "run_after": "2026-01-01T00:00:00Z",
        "run_type": "manual"
      },
      "max_tries": 0,
      "should_retry": false
    }
    """.trimIndent()

  private fun request(mainClass: String = TestSucceedSubprocess::class.java.name): SupervisorRequest {
    val classpath = System.getProperty("java.class.path")
    return SupervisorRequest(
      mainClass = mainClass,
      classpath = classpath,
      executionApiBaseUrl = mockServer.url("/execution/").toString(),
      token = "test-jwt-token",
      workerName = "test-worker",
      userName = "testuser",
      dagRelPath = "dags/test.jar",
      bundleInfo = SupervisorBundleInfo("test-bundle", "1"),
      taskInstance =
        SupervisorTaskInstance(
          id = UUID.randomUUID(),
          taskId = "my_task",
          dagId = "test_dag",
          runId = "run_1",
          tryNumber = 1,
          dagVersionId = UUID.randomUUID(),
          mapIndex = null,
        ),
      sentryIntegration = "",
      onLogLine = {},
    )
  }

  /**
   * A dispatcher that returns a TIRunContext for the /run endpoint and 200 OK for state updates.
   * Also handles variable/connection/xcom API calls for the more complex test scenarios.
   */
  private fun apiDispatcher(): Dispatcher =
    object : Dispatcher() {
      override fun dispatch(request: RecordedRequest): MockResponse {
        val path = request.path ?: return MockResponse().setResponseCode(404)
        return when {
          // tiRun: PATCH .../task-instances/{id}/run
          path.contains("/run") && request.method == "PATCH" ->
            MockResponse()
              .setResponseCode(200)
              .setHeader("Content-Type", "application/json")
              .setBody(tiRunContextJson)

          // succeed/finish: PATCH .../task-instances/{id}/state
          path.contains("/state") && request.method == "PATCH" ->
            MockResponse().setResponseCode(200)

          // getVariable: GET .../variables/{key}
          path.contains("/variables/") && request.method == "GET" ->
            MockResponse()
              .setResponseCode(200)
              .setHeader("Content-Type", "application/json")
              .setBody("""{"key": "test_var", "value": "hello"}""")

          // getConnection: GET .../connections/{id}
          path.contains("/connections/") && request.method == "GET" ->
            MockResponse()
              .setResponseCode(200)
              .setHeader("Content-Type", "application/json")
              .setBody("""{"conn_id": "test_conn", "conn_type": "http"}""")

          // setXcom: POST .../xcoms/...
          path.contains("/xcoms/") && request.method == "POST" ->
            MockResponse()
              .setResponseCode(200)
              .setHeader("Content-Type", "application/json")
              .setBody("{}")

          else ->
            MockResponse().setResponseCode(404).setBody("Not found: $path")
        }
      }
    }

  @Test
  @DisplayName("run: successful task execution returns SUCCESS with exit code 0")
  fun runSuccessfulTask() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      val result = Supervisor.run(request())

      assertEquals(ExecutionTaskInstanceState.SUCCESS, result.finalState)
      assertEquals(0, result.exitCode)
    }

  @Test
  @DisplayName("run: task reporting failed state returns FAILED")
  fun runFailedTask() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      val result = Supervisor.run(request(mainClass = TestFailSubprocess::class.java.name))

      assertEquals(ExecutionTaskInstanceState.FAILED, result.finalState)
      assertEquals(0, result.exitCode) // process exits cleanly, but reports failed state
    }

  @Test
  @DisplayName("run: task requesting a variable before succeeding")
  fun runTaskWithGetVariable() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      val result = Supervisor.run(request(mainClass = TestGetVariableSubprocess::class.java.name))

      assertEquals(ExecutionTaskInstanceState.SUCCESS, result.finalState)
      assertEquals(0, result.exitCode)

      // Verify the variable request was made to the mock server.
      val requests = (1..mockServer.requestCount).map { mockServer.takeRequest() }
      assertTrue(requests.any { it.path?.contains("/variables/") == true })
    }

  @Test
  @DisplayName("run: task requesting a connection before succeeding")
  fun runTaskWithGetConnection() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      val result = Supervisor.run(request(mainClass = TestGetConnectionSubprocess::class.java.name))

      assertEquals(ExecutionTaskInstanceState.SUCCESS, result.finalState)
      assertEquals(0, result.exitCode)

      val requests = (1..mockServer.requestCount).map { mockServer.takeRequest() }
      assertTrue(requests.any { it.path?.contains("/connections/") == true })
    }

  @Test
  @DisplayName("run: reports task as running to execution API with correct payload")
  fun runReportsRunningState() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      Supervisor.run(request())

      val requests = (1..mockServer.requestCount).map { mockServer.takeRequest() }
      val runRequest = requests.first { it.path?.contains("/run") == true && it.method == "PATCH" }
      assertEquals("PATCH", runRequest.method)
      val body = runRequest.body.readUtf8()
      assertTrue(body.contains("test-worker"), "Should contain hostname")
      assertTrue(body.contains("testuser"), "Should contain unix name")
    }

  @Test
  @DisplayName("run: reports terminal state to execution API")
  fun runReportsTerminalState() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      Supervisor.run(request())

      val requests = (1..mockServer.requestCount).map { mockServer.takeRequest() }
      val stateRequest = requests.first { it.path?.contains("/state") == true }
      assertEquals("PATCH", stateRequest.method)
    }

  @Test
  @DisplayName("run: sends bearer token in all API requests")
  fun runSendsBearerToken() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      Supervisor.run(request())

      val requests = (1..mockServer.requestCount).map { mockServer.takeRequest() }
      for (req in requests) {
        val auth = req.getHeader("Authorization")
        assertNotNull(auth, "Authorization header should be present on ${req.path}")
        assertTrue(auth!!.startsWith("Bearer "), "Should use Bearer auth on ${req.path}")
      }
    }

  @Test
  @DisplayName("run: collects stdout and stderr from subprocess")
  fun runCollectsLogLines() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      val logLines = CopyOnWriteArrayList<String>()
      val req =
        request(mainClass = TestStdoutSubprocess::class.java.name).copy(
          onLogLine = { logLines.add(it) },
        )

      Supervisor.run(req)

      assertTrue(logLines.any { it == "stdout-line-1" }, "Should capture stdout: $logLines")
      assertTrue(logLines.any { it == "stdout-line-2" }, "Should capture stdout: $logLines")
      assertTrue(logLines.any { it == "stderr-line-1" }, "Should capture stderr: $logLines")
    }

  @Test
  @DisplayName("run: non-zero exit code overrides final state to FAILED")
  fun runNonZeroExitCodeOverridesState() =
    runBlocking {
      mockServer.dispatcher = apiDispatcher()
      mockServer.start()

      // TestSucceedThenCrashSubprocess sends SucceedTask (which would normally yield SUCCESS)
      // but then exits with code 42. Supervisor should override the final state to FAILED.
      val result = Supervisor.run(request(mainClass = TestSucceedThenCrashSubprocess::class.java.name))

      assertEquals(ExecutionTaskInstanceState.FAILED, result.finalState)
      assertEquals(42, result.exitCode)
    }
}
