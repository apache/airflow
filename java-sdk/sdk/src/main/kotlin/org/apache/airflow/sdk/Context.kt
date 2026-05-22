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

import org.apache.airflow.sdk.execution.StartupDetails

/**
 * Identifies the Dag run that the current task instance belongs to.
 *
 * @property dagId ID of the Dag being run.
 * @property runId Unique identifier for this Dag run.
 */
data class DagRun(
  @JvmField val dagId: String,
  @JvmField val runId: String,
)

/**
 * Identifies the task instance that is currently executing.
 *
 * @property dagId ID of the parent Dag.
 * @property runId ID of the Dag run that triggered this instance.
 * @property taskId ID of the task within the Dag.
 * @property mapIndex Index within a mapped task group, if this is a mapped task instance.
 * @property tryNumber How many times this task instance has been attempted (1-based).
 */
data class TaskInstance(
  @JvmField val dagId: String,
  @JvmField val runId: String,
  @JvmField val taskId: String,
  @JvmField val mapIndex: Int?,
  @JvmField val tryNumber: Int,
)

/**
 * Runtime context passed to the task execution.
 *
 * <p>Provides metadata about the current Dag run and task instance.
 * Use [Client] to interact with Airflow at runtime (connections, variables, XComs).
 *
 * @property dagRun Dag run the currently executing task instance belongs to.
 * @property ti Currently executing task instance.
 */
data class Context(
  @JvmField val dagRun: DagRun,
  @JvmField val ti: TaskInstance,
) {
  internal companion object {
    fun from(request: StartupDetails): Context =
      Context(
        dagRun = with(request.tiContext.dagRun) { DagRun(dagId, runId) },
        ti = with(request.ti) { TaskInstance(dagId, runId, taskId, mapIndex, tryNumber) },
      )
  }
}
