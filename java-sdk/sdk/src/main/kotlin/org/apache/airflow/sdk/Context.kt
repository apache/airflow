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

data class DagRun(
  @JvmField val dagId: String,
  @JvmField val runId: String,
)

data class TaskInstance(
  @JvmField val dagId: String,
  @JvmField val runId: String,
  @JvmField val taskId: String,
  @JvmField val mapIndex: Int?,
  @JvmField val tryNumber: Int,
)

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
