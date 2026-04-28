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
import org.apache.airflow.sdk.Dag
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.execution.api.model.BundleInfo
import org.apache.airflow.sdk.execution.api.model.TIRunContext
import org.apache.airflow.sdk.execution.api.model.TaskInstance
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class TaskRunnerTest {
  @Test
  @DisplayName("Should execute task and return success")
  fun shouldExecuteTaskAndReturnSuccess() {
    val result = TaskRunner.run(bundleWith("success", SuccessTask::class.java), startupDetails(taskId = "success"), noOpClient())

    Assertions.assertInstanceOf(SucceedTask::class.java, result)
  }

  @Test
  @DisplayName("Should return removed when task is missing")
  fun shouldReturnRemovedWhenTaskIsMissing() {
    val result = TaskRunner.run(bundleWith("other", SuccessTask::class.java), startupDetails(taskId = "missing"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals("removed", (result as TaskState).state)
  }

  @Test
  @DisplayName("Should return failed when task throws")
  fun shouldReturnFailedWhenTaskThrows() {
    val result = TaskRunner.run(bundleWith("failing", FailingTask::class.java), startupDetails(taskId = "failing"), noOpClient())

    Assertions.assertInstanceOf(TaskState::class.java, result)
    Assertions.assertEquals("failed", (result as TaskState).state)
  }

  private fun bundleWith(
    taskId: String,
    taskClass: Class<out Task>,
  ): Bundle {
    val dag = Dag("test_dag")
    dag.addTask(taskId, taskClass)
    return Bundle("1", listOf(dag))
  }

  private fun startupDetails(taskId: String): StartupDetails =
    StartupDetails().also {
      it.ti =
        TaskInstance().also { taskInstance ->
          taskInstance.id = UUID.randomUUID()
          taskInstance.taskId = taskId
          taskInstance.dagId = "test_dag"
          taskInstance.runId = "manual__2026-03-31T00:00:00+00:00"
          taskInstance.tryNumber = 1
          taskInstance.dagVersionId = UUID.randomUUID()
        }
      it.dagRelPath = "/dev/null"
      it.bundleInfo =
        BundleInfo().also { info ->
          info.name = "bundle"
          info.version = "1"
        }
      it.startDate = OffsetDateTime.parse("2026-03-31T00:00:00Z")
      it.tiContext = TIRunContext()
      it.sentryIntegration = ""
    }

  private fun noOpClient() =
    Client(
      startupDetails(taskId = "unused"),
      object : org.apache.airflow.sdk.execution.Client {
        override fun getConnection(id: String) = throw UnsupportedOperationException("not used in test")

        override fun getVariable(key: String) = throw UnsupportedOperationException("not used in test")

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
      },
    )

  class SuccessTask : Task {
    override fun execute(client: Client) {
    }
  }

  class FailingTask : Task {
    override fun execute(client: Client): Unit = throw IllegalStateException("boom")
  }
}
