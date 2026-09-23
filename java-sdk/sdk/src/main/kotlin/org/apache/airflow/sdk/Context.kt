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

import org.apache.airflow.sdk.execution.Logger
import org.apache.airflow.sdk.execution.comm.StartupDetails
import java.time.OffsetDateTime
import org.apache.airflow.sdk.execution.comm.DagRun.DagRunType as CommDagRunType

private val logger = Logger(Context::class)

private fun Any?.toDateTime(): OffsetDateTime? =
  when (this) {
    null -> null
    is OffsetDateTime -> this
    is String ->
      runCatching { OffsetDateTime.parse(this) }.getOrElse {
        logger.warning("Ignoring unparsable date-time in run context", mapOf("value" to this))
        null
      }
    else -> {
      logger.warning("Ignoring unexpected date-time value in run context", mapOf("value" to this))
      null
    }
  }

private fun Any?.toConf(): Map<String, Any?> =
  when (this) {
    null -> emptyMap()
    is Map<*, *> -> entries.mapNotNull { (key, value) -> (key as? String)?.let { it to value } }.toMap()
    else -> {
      logger.warning("Ignoring unexpected conf value in run context", mapOf("value" to this))
      emptyMap()
    }
  }

private fun CommDagRunType?.toDagRunType(): DagRunType? =
  when (this) {
    null -> null
    CommDagRunType.BACKFILL -> DagRunType.BACKFILL
    CommDagRunType.SCHEDULED -> DagRunType.SCHEDULED
    CommDagRunType.MANUAL -> DagRunType.MANUAL
    CommDagRunType.OPERATOR_TRIGGERED -> DagRunType.OPERATOR_TRIGGERED
    CommDagRunType.ASSET_TRIGGERED -> DagRunType.ASSET_TRIGGERED
    CommDagRunType.ASSET_MATERIALIZATION -> DagRunType.ASSET_MATERIALIZATION
  }

enum class DagRunType {
  BACKFILL,
  SCHEDULED,
  MANUAL,
  OPERATOR_TRIGGERED,
  ASSET_TRIGGERED,
  ASSET_MATERIALIZATION,
}

/**
 * Identifies the Dag run that the current task instance belongs to.
 *
 * @property dagId ID of the Dag being run.
 * @property runId Unique identifier for this Dag run.
 * @property logicalDate A date-time that logically identifies the current Dag run.
 * @property dataIntervalStart Start of the data interval.
 * @property dataIntervalEnd End of the data interval.
 * @property runAfter A date-time tells the scheduler when the Dag run can be scheduled.
 * @property runType How the run was created.
 * @property conf The configuration for this run.
 */
data class DagRun(
  @JvmField val dagId: String,
  @JvmField val runId: String,
  @JvmField val logicalDate: OffsetDateTime?,
  @JvmField val dataIntervalStart: OffsetDateTime?,
  @JvmField val dataIntervalEnd: OffsetDateTime?,
  @JvmField val runAfter: OffsetDateTime?,
  @JvmField val runType: DagRunType?,
  @JvmField val conf: Map<String, Any?>,
)

/**
 * Identifies the task instance that is currently executing.
 *
 * @property dagId ID of the parent Dag.
 * @property runId ID of the Dag run that triggered this instance.
 * @property taskId ID of the task within the Dag.
 * @property mapIndex Index of a mapped task.
 * @property tryNumber How many times this task instance has been attempted.
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
 * Provides metadata about the current Dag run and task instance. Use [Client]
 * to interact with Airflow at runtime.
 *
 * @property dagRun Dag run the currently executing task instance belongs to.
 * @property ti Currently executing task instance.
 */
data class Context(
  @JvmField val dagRun: DagRun,
  @JvmField val ti: TaskInstance,
) {
  internal companion object {
    fun from(request: StartupDetails) =
      Context(
        dagRun =
          with(request.tiContext.dagRun) {
            DagRun(
              dagId = dagId,
              runId = runId,
              logicalDate = logicalDate.toDateTime(),
              dataIntervalStart = dataIntervalStart.toDateTime(),
              dataIntervalEnd = dataIntervalEnd.toDateTime(),
              runAfter = runAfter.toDateTime(),
              runType = runType.toDagRunType(),
              conf = conf.toConf(),
            )
          },
        ti = with(request.ti) { TaskInstance(dagId, runId, taskId, mapIndex, tryNumber) },
      )
  }
}
