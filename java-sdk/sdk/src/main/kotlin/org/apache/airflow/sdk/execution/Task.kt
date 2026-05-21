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
import org.apache.airflow.sdk.Context

internal object TaskRunner {
  val logger = Logger(TaskRunner::class)

  internal fun runTask(
    bundle: Bundle,
    request: StartupDetails,
    client: Client,
  ): Any {
    val task = bundle.dags[request.ti.dagId]?.tasks[request.ti.taskId] ?: return TaskState("removed")
    val instance = task.getDeclaredConstructor().newInstance()
    return try {
      instance.execute(Context.from(request), client)
      SucceedTask()
    } catch (e: Exception) {
      logger.error("Error executing task", mapOf("ti" to request.ti, "error" to e, "trace" to e.stackTraceToString()))
      e.printStackTrace()
      TaskState("failed")
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
