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

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.TriggerDagRun
import org.apache.airflow.sdk.execution.comm.DagRun
import org.apache.airflow.sdk.execution.comm.DeferTask
import org.apache.airflow.sdk.execution.comm.RetryTask
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.SucceedTask
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.TaskState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

/** One call to [FakeTrigger.triggerDagRun], so a test can read what the task asked Airflow for. */
private class TriggerCall(
  val dagId: String,
  val runId: String,
  val logicalDate: OffsetDateTime?,
  val runAfter: OffsetDateTime?,
  val conf: Map<String, Any?>?,
  val resetDagRun: Boolean,
  val note: String?,
)

private class FakeTrigger(
  private val alreadyExists: Boolean = false,
  private val paused: Boolean = false,
  private val states: List<String> = emptyList(),
) : org.apache.airflow.sdk.execution.Client {
  val triggered = mutableListOf<TriggerCall>()
  val xComs = mutableListOf<Pair<String, Any>>()
  var polls = 0

  override fun triggerDagRun(
    dagId: String,
    runId: String,
    logicalDate: OffsetDateTime?,
    runAfter: OffsetDateTime?,
    conf: Map<String, Any?>?,
    resetDagRun: Boolean,
    note: String?,
  ): Boolean {
    triggered += TriggerCall(dagId, runId, logicalDate, runAfter, conf, resetDagRun, note)
    return alreadyExists
  }

  override fun getDagRunState(
    dagId: String,
    runId: String,
  ): String {
    val started = triggered.last()
    assertEquals(started.dagId, dagId)
    assertEquals(started.runId, runId)
    return states[polls++.coerceAtMost(states.lastIndex)]
  }

  override fun isDagPaused(dagId: String): Boolean = paused

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) {
    xComs += key to value
  }

  override fun getConnection(id: String) = throw UnsupportedOperationException("not used in test")

  override fun getVariable(key: String) = throw UnsupportedOperationException("not used in test")

  override fun setVariable(
    key: String,
    value: String,
    description: String?,
  ): Unit = throw UnsupportedOperationException("not used in test")

  override fun deleteVariable(key: String): Unit = throw UnsupportedOperationException("not used in test")

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ) = throw UnsupportedOperationException("not used in test")

  override fun getTaskStateStore(
    tiId: UUID,
    key: String,
  ) = throw UnsupportedOperationException("not used in test")

  override fun setTaskStateStore(
    tiId: UUID,
    key: String,
    value: Any,
    expiresAt: OffsetDateTime?,
  ): Unit = throw UnsupportedOperationException("not used in test")

  override fun deleteTaskStateStore(
    tiId: UUID,
    key: String,
  ): Unit = throw UnsupportedOperationException("not used in test")

  override fun clearTaskStateStore(tiId: UUID): Unit = throw UnsupportedOperationException("not used in test")

  override fun skipDownstreamTasks(taskIds: List<String>): Unit = throw UnsupportedOperationException("not used in test")
}

/** The trigger time the tests run at, so a generated run ID is the same on every run. */
private val NOW: OffsetDateTime = OffsetDateTime.parse("2026-10-08T05:00:00Z")

internal class TriggerRunnerTest {
  private fun startupDetails(
    nextMethod: String? = null,
    nextKwargs: Any? = null,
    shouldRetry: Boolean = false,
  ): StartupDetails =
    StartupDetails().also {
      it.ti =
        TaskInstance().also { ti ->
          ti.id = UUID.randomUUID()
          ti.taskId = "trigger"
          ti.dagId = "test_dag"
          ti.runId = "manual__2026-03-31T00:00:00+00:00"
          ti.tryNumber = 1
          ti.dagVersionId = UUID.randomUUID()
          ti.queue = "workers"
        }
      it.tiContext =
        TIRunContext().apply {
          dagRun = DagRun().apply { dagId = "test_dag" }
          this.shouldRetry = shouldRetry
          this.nextMethod = nextMethod
          this.nextKwargs = nextKwargs
        }
    }

  private fun run(
    trigger: TriggerDagRun,
    transport: FakeTrigger,
    details: StartupDetails = startupDetails(),
    env: Map<String, String> = emptyMap(),
    slept: MutableList<Duration> = mutableListOf(),
  ): Any =
    TriggerRunner.run(
      trigger,
      details,
      Client(details, transport),
      env = env::get,
      sleep = { slept += it },
      now = { NOW },
      randomSuffix = { "RaNd0m88" },
    )

  @Test
  @DisplayName("Should trigger the run, record its link and succeed without waiting")
  fun shouldTriggerAndSucceed() {
    val transport = FakeTrigger()

    val result =
      run(
        TriggerDagRun("downstream").config("conf", mapOf("rows" to 2)),
        transport,
        env = mapOf("AIRFLOW__API__BASE_URL" to "https://airflow.example.com/"),
      )

    assertInstanceOf(SucceedTask::class.java, result)
    val call = transport.triggered.single()
    assertEquals("downstream", call.dagId)
    assertEquals(mapOf("rows" to 2), call.conf)
    // With neither a logical date nor a run_after, the trigger time is both.
    assertEquals("manual__2026-10-08T05:00:00+00:00", call.runId)
    assertEquals(NOW, call.logicalDate)
    assertNull(call.runAfter)
    assertEquals(
      listOf(
        "_link_TriggerDagRunLink" to "https://airflow.example.com/dags/downstream/runs/${call.runId}",
        "trigger_run_id" to call.runId,
      ),
      transport.xComs,
    )
  }

  @Test
  @DisplayName("Should trigger the run under the ID and settings the task carries")
  fun shouldUseConfiguredRunId() {
    val transport = FakeTrigger()
    val logicalDate = OffsetDateTime.parse("2026-09-30T00:00:00Z")

    run(
      TriggerDagRun("downstream")
        .config("trigger_run_id", "fixed")
        .config("logical_date", logicalDate)
        .config("run_after", logicalDate)
        .config("reset_dag_run", true)
        .config("note", "from java"),
      transport,
    )

    val call = transport.triggered.single()
    assertEquals("fixed", call.runId)
    assertEquals(logicalDate, call.logicalDate)
    assertEquals(logicalDate, call.runAfter)
    assertTrue(call.resetDagRun)
    assertEquals("from java", call.note)
  }

  @Test
  @DisplayName("Should name the run after the logical date the task set")
  fun shouldNameTheRunAfterTheLogicalDate() {
    val transport = FakeTrigger()
    val logicalDate = OffsetDateTime.parse("2026-09-30T00:00:00Z")

    run(TriggerDagRun("downstream").config("logical_date", logicalDate), transport)

    val call = transport.triggered.single()
    assertEquals("manual__2026-09-30T00:00:00+00:00", call.runId)
    assertEquals(logicalDate, call.logicalDate)
    assertNull(call.runAfter)
  }

  @Test
  @DisplayName("Should leave a run with only a run_after no logical date, and add a random suffix")
  fun shouldLeaveRunAfterOnlyRunWithoutALogicalDate() {
    val transport = FakeTrigger()
    val runAfter = OffsetDateTime.parse("2026-12-01T00:00:00Z")

    run(TriggerDagRun("downstream").config("run_after", runAfter), transport)

    val call = transport.triggered.single()
    assertEquals("manual__2026-12-01T00:00:00+00:00_RaNd0m88", call.runId)
    assertNull(call.logicalDate)
    assertEquals(runAfter, call.runAfter)
  }

  @Test
  @DisplayName("Should skip rather than fail an existing run when the task says so")
  fun shouldSkipWhenAlreadyExists() {
    val transport = FakeTrigger(alreadyExists = true)

    val result = run(TriggerDagRun("downstream").config("skip_when_already_exists", true), transport)

    assertEquals(TaskState.State.SKIPPED, (result as TaskState).state)
  }

  @Test
  @DisplayName("Should fail an existing run the task does not skip")
  fun shouldFailWhenAlreadyExists() {
    val transport = FakeTrigger(alreadyExists = true)

    val result = run(TriggerDagRun("downstream"), transport)

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    assertEquals(listOf("_link_TriggerDagRunLink"), transport.xComs.map { it.first })
  }

  @Test
  @DisplayName("Should refuse to trigger a paused Dag when the task says so")
  fun shouldFailWhenDagIsPaused() {
    val transport = FakeTrigger(paused = true)

    val result = run(TriggerDagRun("downstream").config("fail_when_dag_is_paused", true), transport)

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    assertTrue(transport.triggered.isEmpty())
  }

  @Test
  @DisplayName("Should trigger a Dag that is not paused though the task would fail on a paused one")
  fun shouldTriggerUnpausedDagWhenFailingOnPaused() {
    val transport = FakeTrigger(paused = false)

    val result = run(TriggerDagRun("downstream").config("fail_when_dag_is_paused", true), transport)

    assertInstanceOf(SucceedTask::class.java, result)
    assertEquals(listOf("downstream"), transport.triggered.map { it.dagId })
  }

  @Test
  @DisplayName("Should wait until the triggered run reaches an allowed state")
  fun shouldWaitForCompletion() {
    val transport = FakeTrigger(states = listOf("running", "success"))
    val slept = mutableListOf<Duration>()

    val result =
      run(
        TriggerDagRun("downstream")
          .config("wait_for_completion", true)
          .config("poke_interval", Duration.ofSeconds(5)),
        transport,
        slept = slept,
      )

    assertInstanceOf(SucceedTask::class.java, result)
    assertEquals(listOf(Duration.ofSeconds(5), Duration.ofSeconds(5)), slept)
  }

  @Test
  @DisplayName("Should retry when the triggered run reaches a failed state and the task has tries left")
  fun shouldFailWhenRunFails() {
    val transport = FakeTrigger(states = listOf("failed"))

    val result =
      run(
        TriggerDagRun("downstream").config("wait_for_completion", true).config("poke_interval", Duration.ZERO),
        transport,
        details = startupDetails(shouldRetry = true),
      )

    assertInstanceOf(RetryTask::class.java, result)
  }

  @Test
  @DisplayName("Should defer the wait to the triggerer when the task is deferrable")
  fun shouldDefer() {
    val transport = FakeTrigger()

    val result =
      run(
        TriggerDagRun("downstream")
          .config("wait_for_completion", true)
          .config("deferrable", true)
          .config("poke_interval", Duration.ofSeconds(30))
          .config("failed_states", listOf("failed", "queued")),
        transport,
        env = mapOf("AIRFLOW__TRIGGERER__QUEUES_ENABLED" to "true"),
      )

    val defer = assertInstanceOf(DeferTask::class.java, result)
    assertEquals("airflow.providers.standard.triggers.external_task.DagStateTrigger", defer.classpath)
    assertEquals("execute_complete", defer.nextMethod)
    assertEquals("workers", defer.queue)
    assertEquals(
      mapOf(
        "dag_id" to "downstream",
        "states" to listOf("success", "failed", "queued"),
        "poll_interval" to 30,
        "run_ids" to listOf(transport.triggered.single().runId),
        "execution_dates" to null,
      ),
      defer.triggerKwargs,
    )
  }

  @Test
  @DisplayName("Should follow the default deferrable setting when the task sets none")
  fun shouldDeferByDefaultWhenConfigured() {
    val result =
      run(
        TriggerDagRun("downstream").config("wait_for_completion", true),
        FakeTrigger(),
        env = mapOf("AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE" to "true"),
      )

    assertInstanceOf(DeferTask::class.java, result)
  }

  @Test
  @DisplayName("Should leave the deferred task's queue unset when triggerer queues are not enabled")
  fun shouldNotQueueDeferralWithoutTriggererQueues() {
    val result =
      run(
        TriggerDagRun("downstream").config("wait_for_completion", true).config("deferrable", true),
        FakeTrigger(),
      )

    assertNull(assertInstanceOf(DeferTask::class.java, result).queue)
  }

  @Test
  @DisplayName("Should not defer a task that does not wait")
  fun shouldNotDeferWithoutWaiting() {
    val transport = FakeTrigger()

    val result = run(TriggerDagRun("downstream").config("deferrable", true), transport)

    assertInstanceOf(SucceedTask::class.java, result)
  }

  @Test
  @DisplayName("Should succeed when the triggerer reports an allowed state")
  fun shouldResumeWithAllowedState() {
    val transport = FakeTrigger()
    val event =
      mapOf(
        "__classname__" to "builtins.tuple",
        "__data__" to listOf("unused", mapOf("run_ids" to listOf("r1"), "r1" to "success")),
      )

    val result =
      run(
        TriggerDagRun("downstream").config("wait_for_completion", true),
        transport,
        details = startupDetails(nextMethod = "execute_complete", nextKwargs = mapOf("event" to event)),
      )

    assertInstanceOf(SucceedTask::class.java, result)
    assertTrue(transport.triggered.isEmpty())
  }

  @Test
  @DisplayName("Should fail when the triggerer reports a failed state")
  fun shouldResumeWithFailedState() {
    val transport = FakeTrigger()
    val event = listOf("unused", mapOf("run_ids" to listOf("r1"), "r1" to "failed"))

    val result =
      run(
        TriggerDagRun("downstream").config("wait_for_completion", true),
        transport,
        details = startupDetails(nextMethod = "execute_complete", nextKwargs = mapOf("event" to event)),
      )

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
  }

  @Test
  @DisplayName("Should fail when the trigger itself failed")
  fun shouldResumeFromTriggerFailure() {
    val result =
      run(
        TriggerDagRun("downstream"),
        FakeTrigger(),
        details =
          startupDetails(
            nextMethod = "__fail__",
            nextKwargs = mapOf("error" to "boom", "traceback" to listOf("line one", "line two")),
          ),
      )

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
  }

  @Test
  @DisplayName("Should fail when resumed with a method it does not know")
  fun shouldRejectUnknownResumeMethod() {
    val result = run(TriggerDagRun("downstream"), FakeTrigger(), details = startupDetails(nextMethod = "other"))

    assertEquals(TaskState.State.FAILED, (result as TaskState).state)
  }
}
