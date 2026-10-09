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

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.ConditionTask
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.InputTask
import org.apache.airflow.sdk.SwitchTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.TriggerDagRun
import org.apache.airflow.sdk.execution.comm.BundleInfo
import org.apache.airflow.sdk.execution.comm.DagRun
import org.apache.airflow.sdk.execution.comm.RetryTask
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.SucceedTask
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.TaskState
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.time.OffsetDateTime
import java.util.UUID

class TaskTest {
  @Test
  @DisplayName("Should execute task and return success")
  fun shouldExecuteTaskAndReturnSuccess() {
    val result = runTask(bundleWith("success", SuccessTask::class.java), startupDetails(taskId = "success"), noOpClient())

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
  }

  @Test
  @DisplayName("Should return removed without deleting XComs when task is missing")
  fun shouldReturnRemovedWithoutDeletingXComsWhenTaskIsMissing() {
    val details = startupDetails(taskId = "missing", xcomKeysToClear = listOf("return_value"))
    val transport = RecordingTransport()
    val result = runTask(bundleWith("other", SuccessTask::class.java), details, Client(details, transport))

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.REMOVED, (result as TaskState).state)
    Assertions.assertEquals(emptyList<String>(), transport.events)
  }

  @Test
  @DisplayName("Should return failed when task throws")
  fun shouldReturnFailedWhenTaskThrows() {
    val result = runTask(bundleWith("failing", FailingTask::class.java), startupDetails(taskId = "failing"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
  }

  @Test
  @DisplayName("Should return retry when task throws and should_retry is true")
  fun shouldReturnRetryWhenTaskThrowsAndShouldRetryIsTrue() {
    val details = startupDetails(taskId = "failing")
    details.tiContext?.shouldRetry = true
    val result = runTask(bundleWith("failing", FailingTask::class.java), details, noOpClient())

    Assertions.assertInstanceOf(RetryTask::class.java, result)
  }

  @Test
  @DisplayName("Should return failed when task throws an Error")
  fun shouldReturnFailedWhenTaskThrowsError() {
    val result = runTask(bundleWith("erroring", ErrorThrowingTask::class.java), startupDetails(taskId = "erroring"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
  }

  @Test
  @DisplayName("Should not duplicate the stack trace to stderr when task fails")
  fun shouldNotDuplicateStackTraceToStderrWhenTaskFails() {
    val captured = ByteArrayOutputStream()
    val original = System.err
    System.setErr(PrintStream(captured))
    try {
      runTask(bundleWith("failing", FailingTask::class.java), startupDetails(taskId = "failing"), noOpClient())
    } finally {
      System.setErr(original)
    }

    Assertions.assertEquals("", captured.toString())
  }

  @Test
  @DisplayName("Should return failed and log an actionable message when task class has no public no-argument constructor")
  fun shouldReturnFailedWhenTaskClassHasNoNoArgConstructor() {
    LogSender.messages.clear()
    val result =
      runTask(bundleWith("uninstantiable", NoDefaultConstructorTask::class.java), startupDetails(taskId = "uninstantiable"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    val message = LogSender.messages.single { it.level == Level.ERROR }
    Assertions.assertTrue(message.event.contains("public no-argument constructor")) { "unexpected event: ${message.event}" }
    Assertions.assertEquals(NoDefaultConstructorTask::class.java.name, message.arguments["taskClass"])
  }

  @Test
  @DisplayName("Should return failed and log the constructor failure when task class constructor throws")
  fun shouldReturnFailedWhenTaskClassConstructorThrows() {
    LogSender.messages.clear()
    val result =
      runTask(bundleWith("throwing", ThrowingConstructorTask::class.java), startupDetails(taskId = "throwing"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    val message = LogSender.messages.single { it.level == Level.ERROR }
    Assertions.assertEquals("Task class constructor threw an exception", message.event)
    Assertions.assertEquals(ThrowingConstructorTask::class.java.name, message.arguments["taskClass"])
    Assertions.assertInstanceOf(IllegalStateException::class.java, message.arguments["error"])
    Assertions.assertEquals("constructor boom", (message.arguments["error"] as Throwable).message)
  }

  @Test
  @DisplayName("Should return failed and log an initialization failure when the task class static initializer throws")
  fun shouldReturnFailedWhenTaskClassStaticInitializerThrows() {
    LogSender.messages.clear()
    val result =
      runTask(bundleWith("static_init", StaticInitFailureTask::class.java), startupDetails(taskId = "static_init"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    val message = LogSender.messages.single { it.level == Level.ERROR }
    Assertions.assertEquals("Error initializing task class", message.event)
    Assertions.assertEquals(StaticInitFailureTask::class.java.name, message.arguments["taskClass"])
  }

  @Test
  @DisplayName("Should return retry when the task class fails to initialize and should_retry is true")
  fun shouldReturnRetryWhenTaskClassFailsToInitializeAndShouldRetryIsTrue() {
    val details = startupDetails(taskId = "static_init")
    details.tiContext?.shouldRetry = true
    val result = runTask(bundleWith("static_init", StaticInitFailureTask::class.java), details, noOpClient())

    Assertions.assertInstanceOf(RetryTask::class.java, result)
  }

  @Test
  @DisplayName("Should return failed and never retry when task class cannot be instantiated, even if should_retry is true")
  fun shouldReturnFailedEvenIfShouldRetryIsTrueWhenTaskClassCannotBeInstantiated() {
    for (taskClass in listOf(NoDefaultConstructorTask::class.java, ThrowingConstructorTask::class.java)) {
      val details = startupDetails(taskId = "uninstantiable")
      details.tiContext?.shouldRetry = true
      val result = runTask(bundleWith("uninstantiable", taskClass), details, noOpClient())

      Assertions.assertInstanceOf(TaskState::class.java, result) { "unexpected result for ${taskClass.simpleName}: $result" }
      Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    }
  }

  @Test
  @DisplayName("Should thread the task definition into the execution context")
  fun shouldThreadTaskDefIntoContext() {
    val result =
      runTask(
        bundleWith("asserting", TaskDefAssertingTask::class.java),
        startupDetails(taskId = "asserting"),
        noOpClient(),
      )

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
  }

  @Test
  @DisplayName("Should push a condition's result and skip the side it did not take")
  fun shouldSkipTheSideNotTaken() {
    TestCondition.decision = true
    val transport = RecordingTransport()

    val result =
      runTask(conditionBundle(withElse = true), startupDetails(taskId = "gate"), Client(startupDetails("gate"), transport))

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
    Assertions.assertEquals(listOf("report_empty"), transport.skipped)
    Assertions.assertEquals(
      listOf("skipmixin_key" to mapOf("skipped" to listOf("report_empty")), "return_value" to true),
      transport.xComs,
    )
    Assertions.assertEquals(
      listOf("xcom:skipmixin_key", "skip:[report_empty]", "xcom:return_value"),
      transport.events,
    )
  }

  @Test
  @DisplayName("Should skip the then side when a condition does not hold")
  fun shouldSkipThenSideWhenConditionIsFalse() {
    TestCondition.decision = false
    val transport = RecordingTransport()

    runTask(conditionBundle(withElse = true), startupDetails(taskId = "gate"), Client(startupDetails("gate"), transport))

    Assertions.assertEquals(listOf("load"), transport.skipped)
    Assertions.assertEquals(false, transport.xComs.last().second)
    Assertions.assertEquals(listOf("xcom:skipmixin_key", "skip:[load]", "xcom:return_value"), transport.events)
  }

  @Test
  @DisplayName("Should skip nothing when a one-sided condition holds")
  fun shouldSkipNothingWhenOneSidedConditionHolds() {
    TestCondition.decision = true
    val transport = RecordingTransport()

    runTask(conditionBundle(withElse = false), startupDetails(taskId = "gate"), Client(startupDetails("gate"), transport))

    Assertions.assertEquals(emptyList<String>(), transport.skipped)
    Assertions.assertEquals(listOf("return_value" to true), transport.xComs)
    Assertions.assertEquals(listOf("xcom:return_value"), transport.events)
  }

  @Test
  @DisplayName("Should fail a condition that throws, without pushing or skipping anything")
  fun shouldFailConditionThatThrows() {
    TestCondition.decision = null
    val transport = RecordingTransport()

    val result =
      runTask(conditionBundle(withElse = true), startupDetails(taskId = "gate"), Client(startupDetails("gate"), transport))

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    Assertions.assertEquals(emptyList<String>(), transport.skipped)
    Assertions.assertEquals(emptyList<Pair<String, Any>>(), transport.xComs)
  }

  @Test
  @DisplayName("Should push the case a switch chose and skip every other one")
  fun shouldSkipTheCasesNotChosen() {
    TestSwitch.choice = HandleLongTask::class.java
    val transport = RecordingTransport()

    val result = runTask(switchBundle(), startupDetails(taskId = "pick"), Client(startupDetails("pick"), transport))

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
    Assertions.assertEquals(listOf("handle_short"), transport.skipped)
    Assertions.assertEquals(
      listOf("skipmixin_key" to mapOf("skipped" to listOf("handle_short")), "return_value" to "handle_long"),
      transport.xComs,
    )
  }

  @Test
  @DisplayName("Should fail a switch that chose a task it cannot run")
  fun shouldFailSwitchThatChoseAnUnknownCase() {
    TestSwitch.choice = SuccessTask::class.java
    val transport = RecordingTransport()

    val result = runTask(switchBundle(), startupDetails(taskId = "pick"), Client(startupDetails("pick"), transport))

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    Assertions.assertEquals(emptyList<String>(), transport.skipped)
    Assertions.assertEquals(emptyList<Pair<String, Any>>(), transport.xComs)
  }

  @Test
  @DisplayName("Should fail a generated switch that chose a task of the Dag that is not one of its cases")
  fun shouldFailGeneratedSwitchThatChoseANonCase() {
    val dag = DagDef("test_dag")
    val long = dag.task<Unit>("handle_long", HandleLongTask::class.java)
    val short = dag.task<Unit>("handle_short", HandleShortTask::class.java)
    dag.task<Unit>("unlisted", UnlistedTask::class.java)
    dag.Switch("pick", TestGeneratedSwitch::class.java).Case(long).Case(short)
    val transport = RecordingTransport()

    val result = runTask(Bundle(listOf(dag)), startupDetails(taskId = "pick"), Client(startupDetails("pick"), transport))

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    Assertions.assertEquals(emptyList<String>(), transport.skipped)
    Assertions.assertEquals(emptyList<Pair<String, Any>>(), transport.xComs)
  }

  @Test
  @DisplayName("Should run a trigger task itself and push the ID of the run it started")
  fun shouldRunTriggerTask() {
    val transport = RecordingTransport()

    val result = runTask(triggerBundle(), startupDetails(taskId = "trigger"), Client(startupDetails("trigger"), transport))

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
    Assertions.assertEquals(listOf("downstream"), transport.triggered)
    Assertions.assertTrue("trigger_run_id" in transport.xComs.map { it.first }) { "pushed: ${transport.xComs}" }
  }

  @Test
  @DisplayName("Should fail a trigger task whose client throws")
  fun shouldFailTriggerTaskWhenClientThrows() {
    val transport = RecordingTransport().also { it.triggerFailure = IllegalStateException("boom") }

    val result = runTask(triggerBundle(), startupDetails(taskId = "trigger"), Client(startupDetails("trigger"), transport))

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
    Assertions.assertFalse("trigger_run_id" in transport.xComs.map { it.first })
  }

  private fun triggerBundle(): Bundle {
    val dag = DagDef("test_dag")
    dag.task("trigger", TriggerDagRun("downstream"))
    return Bundle(listOf(dag))
  }

  private fun switchBundle(): Bundle {
    val dag = DagDef("test_dag")
    val long = dag.task<Unit>("handle_long", HandleLongTask::class.java)
    val short = dag.task<Unit>("handle_short", HandleShortTask::class.java)
    dag.Switch("pick", TestSwitch::class.java).Case(long).Case(short)
    return Bundle(listOf(dag))
  }

  private fun conditionBundle(withElse: Boolean): Bundle {
    val dag = DagDef("test_dag")
    val load = dag.task<Unit>("load", SuccessTask::class.java)
    val reportEmpty = dag.task<Unit>("report_empty", SuccessTask::class.java)
    val condition = dag.If("gate", TestCondition::class.java).Then(load)
    if (withElse) condition.Else(reportEmpty)
    return Bundle(listOf(dag))
  }

  @Test
  @DisplayName("Should delete XComs before instantiating the task class and binding its arguments")
  fun shouldDeleteXComsBeforeInstantiatingTaskClassAndBindingArguments() {
    val failures =
      listOf(
        ThrowingConstructorTask::class.java to null,
        BindingTask::class.java to listOf(mapOf("kind" to "bogus", "name" to "region")),
      )
    for ((taskClass, argBindings) in failures) {
      val details =
        startupDetails(taskId = "early", xcomKeysToClear = listOf("return_value", "summary"), argBindings = argBindings)
      val transport = RecordingTransport()
      val result = runTask(bundleWith("early", taskClass), details, Client(details, transport))

      Assertions.assertInstanceOf(TaskState::class.java, result) { "unexpected result for ${taskClass.simpleName}: $result" }
      Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
      Assertions.assertEquals(listOf("delete:return_value", "delete:summary"), transport.events) {
        "unexpected events for ${taskClass.simpleName}"
      }
    }
  }

  @Test
  @DisplayName("Should fail without running the task and log the key when an XCom cannot be deleted")
  fun shouldFailAndLogTheKeyWhenAnXComCannotBeDeleted() {
    for (error in listOf(IllegalStateException("database is down"), NoClassDefFoundError("simulated"))) {
      LogSender.messages.clear()
      val details = startupDetails(taskId = "success", xcomKeysToClear = listOf("return_value", "summary"))
      val transport = RecordingTransport(deleteError = error)
      val result = runTask(bundleWith("success", SuccessTask::class.java), details, Client(details, transport))

      Assertions.assertInstanceOf(TaskState::class.java, result) { "unexpected result for $error: $result" }
      Assertions.assertEquals(TaskState.State.FAILED, (result as TaskState).state)
      Assertions.assertEquals(listOf("delete:return_value"), transport.events)
      val message = LogSender.messages.single { it.level == Level.ERROR }
      Assertions.assertEquals("Error clearing XCom", message.event)
      Assertions.assertEquals("return_value", message.arguments["key"])
      Assertions.assertSame(error, message.arguments["error"])
    }
  }

  @Test
  @DisplayName("Should delete XComs before a trigger task starts the Dag run")
  fun shouldDeleteXComsBeforeTriggerTaskRuns() {
    val details = startupDetails(taskId = "trigger", xcomKeysToClear = listOf("trigger_run_id"))
    val transport = RecordingTransport()

    val result = runTask(triggerBundle(), details, Client(details, transport))

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
    Assertions.assertEquals("delete:trigger_run_id", transport.events.first()) { "events: ${transport.events}" }
    Assertions.assertEquals(listOf("downstream"), transport.triggered)
  }

  private fun bundleWith(
    taskId: String,
    taskClass: Class<out Task>,
  ): Bundle {
    val dag = DagDef("test_dag").addTask(TaskDef(taskId, taskClass))
    return Bundle(listOf(dag))
  }

  private fun startupDetails(
    taskId: String,
    xcomKeysToClear: List<String> = emptyList(),
    argBindings: List<Map<String, Any?>>? = null,
  ): StartupDetails =
    StartupDetails().also {
      it.ti =
        TaskInstance().also { o ->
          o.id = UUID.randomUUID()
          o.taskId = taskId
          o.dagId = "test_dag"
          o.runId = "manual__2026-03-31T00:00:00+00:00"
          o.tryNumber = 1
          o.dagVersionId = UUID.randomUUID()
        }
      it.dagRelPath = "/dev/null"
      it.bundleInfo =
        BundleInfo().also { info ->
          info.name = "bundle"
          info.version = "1"
        }
      it.startDate = OffsetDateTime.parse("2026-03-31T00:00:00Z")
      it.tiContext =
        TIRunContext().apply {
          dagRun =
            DagRun().apply {
              dagId = "test_dag"
              runId = "manual__2026-03-31T00:00:00+00:00"
            }
          this.xcomKeysToClear = xcomKeysToClear
          this.argBindings = argBindings
        }
      it.sentryIntegration = ""
    }

  private fun noOpClient() = Client(startupDetails(taskId = "unused"), NoOpTransport)

  private object NoOpTransport : org.apache.airflow.sdk.execution.Client {
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

    override fun setXCom(
      key: String,
      value: Any,
      dagId: String,
      taskId: String,
      runId: String,
      mapIndex: Int,
    ): Unit = throw UnsupportedOperationException("not used in test")

    override fun deleteXCom(
      key: String,
      dagId: String,
      taskId: String,
      runId: String,
      mapIndex: Int,
    ): Unit = throw UnsupportedOperationException("not used in test")

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

    override fun triggerDagRun(
      dagId: String,
      runId: String,
      logicalDate: OffsetDateTime?,
      runAfter: OffsetDateTime?,
      conf: Map<String, Any?>?,
      resetDagRun: Boolean,
      note: String?,
    ): Boolean = throw UnsupportedOperationException("not used in test")

    override fun getDagRunState(
      dagId: String,
      runId: String,
    ): String = throw UnsupportedOperationException("not used in test")

    override fun isDagPaused(dagId: String): Boolean = throw UnsupportedOperationException("not used in test")
  }

  /** Decides what [decision] holds; a null one throws, standing for a condition body that fails. */
  class TestCondition : ConditionTask {
    override fun decide(
      context: Context,
      client: Client,
    ): Boolean = decision ?: throw IllegalStateException("boom")

    companion object {
      var decision: Boolean? = true
    }
  }

  class HandleLongTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  class HandleShortTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  /** Chooses whatever [choice] holds, so a test can drive it to a case or past one. */
  class TestSwitch : SwitchTask {
    override fun choose(
      context: Context,
      client: Client,
    ): Class<out Task> = choice

    companion object {
      var choice: Class<out Task> = HandleLongTask::class.java
    }
  }

  class UnlistedTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  /** The shape the annotation processor generates: it returns the class of a task, here one that is no case. */
  class TestGeneratedSwitch : SwitchTask {
    override fun choose(
      context: Context,
      client: Client,
    ): Class<out Task> = UnlistedTask::class.java
  }

  /** Records what a deciding task pushed and asked to skip, and the order of the two. */
  private class RecordingTransport(
    private val deleteError: Throwable? = null,
  ) : org.apache.airflow.sdk.execution.Client by NoOpTransport {
    val xComs = mutableListOf<Pair<String, Any>>()
    val skipped = mutableListOf<String>()
    val events = mutableListOf<String>()
    val triggered = mutableListOf<String>()
    var triggerFailure: Throwable? = null

    override fun setXCom(
      key: String,
      value: Any,
      dagId: String,
      taskId: String,
      runId: String,
      mapIndex: Int,
    ) {
      xComs += key to value
      events += "xcom:$key"
    }

    override fun deleteXCom(
      key: String,
      dagId: String,
      taskId: String,
      runId: String,
      mapIndex: Int,
    ) {
      events += "delete:$key"
      deleteError?.let { throw it }
    }

    override fun skipDownstreamTasks(taskIds: List<String>) {
      skipped += taskIds
      events += "skip:$taskIds"
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
      triggered += dagId
      triggerFailure?.let { throw it }
      return false
    }
  }

  class SuccessTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) {
    }
  }

  class FailingTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ): Unit = throw IllegalStateException("boom")
  }

  class ErrorThrowingTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ): Unit = throw NoClassDefFoundError("simulated")
  }

  class ThrowingConstructorTask : Task {
    init {
      throw IllegalStateException("constructor boom")
    }

    override fun execute(
      context: Context,
      client: Client,
    ) {
    }
  }

  class StaticInitFailureTask : Task {
    companion object {
      init {
        throw IllegalStateException("static init boom")
      }
    }

    override fun execute(
      context: Context,
      client: Client,
    ) {
    }
  }

  class NoDefaultConstructorTask(
    unused: String,
  ) : Task {
    override fun execute(
      context: Context,
      client: Client,
    ): Unit = throw IllegalStateException("should not be reachable")
  }

  class TaskDefAssertingTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) {
      check(context.taskDef?.id == context.ti.taskId) {
        "expected the runner to thread the task definition into the context"
      }
    }
  }

  class BindingInput : TaskInput {
    @JvmField
    var region: String? = null
  }

  class BindingTask : InputTask<BindingInput> {
    override fun execute(
      context: Context,
      client: Client,
      input: BindingInput,
    ) {
      client.setXCom(value = "ran")
    }
  }
}
