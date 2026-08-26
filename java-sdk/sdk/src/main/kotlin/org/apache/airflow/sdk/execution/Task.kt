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

import kotlinx.coroutines.CancellationException
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.execution.comm.AssetProfile
import org.apache.airflow.sdk.execution.comm.RetryTask
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.SucceedTask
import org.apache.airflow.sdk.execution.comm.TaskState
import java.lang.reflect.InvocationTargetException
import java.time.OffsetDateTime

internal object TaskResult {
  fun success(
    endDate: OffsetDateTime = OffsetDateTime.now(),
    taskOutlets: List<AssetProfile> = emptyList(),
    outletEvents: List<Map<String, Any?>> = emptyList(),
    renderedMapIndex: String? = null,
  ) = SucceedTask().also {
    it.state = "success"
    it.endDate = endDate
    it.taskOutlets = taskOutlets
    it.outletEvents = outletEvents
    it.renderedMapIndex = renderedMapIndex
  }

  fun retry(
    endDate: OffsetDateTime = OffsetDateTime.now(),
    renderedMapIndex: String? = null,
  ) = RetryTask().also {
    it.endDate = endDate
    it.renderedMapIndex = renderedMapIndex
  }

  fun of(
    state: TaskState.State,
    endDate: OffsetDateTime = OffsetDateTime.now(),
    renderedMapIndex: String? = null,
  ) = TaskState().also {
    it.state = state
    it.endDate = endDate
    it.renderedMapIndex = renderedMapIndex
  }

  fun failure(shouldRetry: Boolean) = if (shouldRetry) retry() else of(TaskState.State.FAILED)
}

internal object TaskRunner {
  val logger = Logger(TaskRunner::class)

  internal fun runTask(
    bundle: Bundle,
    request: StartupDetails,
    client: Client,
  ): Any {
    val definition =
      bundle.dags[request.ti.dagId]?.tasks[request.ti.taskId]?.definition
        ?: return TaskResult.of(TaskState.State.REMOVED)
    val instance =
      try {
        definition.getDeclaredConstructor().newInstance()
      } catch (e: InvocationTargetException) {
        val cause = e.cause ?: e
        logger.error(
          "Task class constructor threw an exception",
          mapOf("ti" to request.ti, "taskClass" to definition.name, "error" to cause, "trace" to cause.stackTraceToString()),
        )
        // Retrying cannot help: instantiation fails the same way on every try.
        return TaskResult.failure(shouldRetry = false)
      } catch (e: ReflectiveOperationException) {
        logger.error(
          "Cannot instantiate task class. A task class must be public, concrete, and declare a public no-argument constructor",
          mapOf("ti" to request.ti, "taskClass" to definition.name, "error" to e, "trace" to e.stackTraceToString()),
        )
        return TaskResult.failure(shouldRetry = false)
      } catch (e: Throwable) {
        // A valid class can still fail to initialize (static initializer, linkage); a fresh JVM may succeed.
        logger.error(
          "Error initializing task class",
          mapOf("ti" to request.ti, "taskClass" to definition.name, "error" to e, "trace" to e.stackTraceToString()),
        )
        return TaskResult.failure(request.tiContext.shouldRetry)
      }
    return try {
      instance.execute(Context.from(request), client)
      TaskResult.success()
    } catch (e: CancellationException) {
      throw e // Let coroutine cancellation propagate so the task coroutine unwinds.
    } catch (e: Throwable) {
      logger.error("Error executing task", mapOf("ti" to request.ti, "error" to e, "trace" to e.stackTraceToString()))
      TaskResult.failure(request.tiContext.shouldRetry)
    }
  }
}

internal fun runTask(
  bundle: Bundle,
  request: StartupDetails,
  comm: CoordinatorComm,
): Any = TaskRunner.runTask(bundle, request, Client(request, CoordinatorClient(comm)))

internal fun runTask(
  bundle: Bundle,
  request: StartupDetails,
  client: Client,
) = TaskRunner.runTask(bundle, request, client)
