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
import org.apache.airflow.sdk.internal.bindConf
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
 * @property conf The configuration for this run. [getConf] reads it into an
 *    instance of a class with typed fields.
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
) {
  /**
   * Reads [conf] into a new instance of [type]. For example, [type] can be a
   * class with one field per conf key.
   *
   * ```java
   * public static class EtlConf {
   *   public String targetTable; // set from the "target_table" key
   *   public int limit = 100;    // stays 100 when the conf has no "limit" key
   * }
   *
   * EtlConf etl = context.dagRun.getConf(EtlConf.class);
   * ```
   *
   * Each key goes to the field, setter or record component whose name matches
   * the key when case and underscores are ignored. If one key matches a name
   * exactly, that key wins over keys that match the same name only when case
   * and underscores are ignored. Objects nested in the conf follow the same
   * rules, including objects in lists and objects that are map values. The keys
   * of a map field stay as written.
   *
   * Keys that match no field are ignored, because other tasks in the run may
   * read them. A field with no matching key keeps its default value. The conf
   * holds only the values that the run was triggered with. This method does not
   * add the defaults of the Dag's params, so in a scheduled run, where the conf
   * is empty, no field has a matching key.
   *
   * The class needs a no-argument constructor and public fields or setters, so
   * a nested class must be `static`. A record also works. A record component
   * with no matching key is null. If the component has a primitive type, this
   * method throws instead, so a task that reads the conf into a record with a
   * primitive component fails in a scheduled run. In Kotlin, declare the fields
   * with `@JvmField` so that a property such as `isEnabled` keeps its name.
   *
   * Annotating a field with [ArgName] has no effect on this method. To set a
   * field from a key that no Java name can match, such as `target-table`, add
   * `@JsonProperty("target-table")` to that field. `@JsonProperty` needs
   * `com.fasterxml.jackson.core:jackson-annotations` in your build, because the
   * SDK keeps Jackson off your compile classpath. You can also read the key
   * from [conf].
   *
   * This method throws [IllegalArgumentException] in these cases:
   *
   * * A value, such as an empty string for an `int` field, cannot be converted
   *   to the type of its field.
   * * A key holds null for a primitive field, a list holds null for a primitive
   *   array such as `int[]`, or a primitive record component has no matching
   *   key. To let a field or a component receive null, declare it with a boxed
   *   type, such as `Integer`.
   * * Two keys, such as `target_table` and `TARGET_TABLE`, match the same field
   *   after case and underscores are ignored, but neither key matches the field
   *   name exactly.
   * * The class has two fields whose names differ only in case or underscores,
   *   such as `targetTable` and `target_table`. This method checks the class
   *   even when the conf is empty, so it throws in a scheduled run too.
   *
   * @param type Class to read the conf into.
   * @throws IllegalArgumentException in the cases listed above.
   */
  fun <T : Any> getConf(type: Class<T>): T = bindConf(conf, type)
}

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
  /** Registration of the executing task; resolves wired data inputs. */
  internal var taskDef: TaskDef? = null

  internal companion object {
    fun from(
      request: StartupDetails,
      taskDef: TaskDef? = null,
    ): Context =
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
      ).also { it.taskDef = taskDef }
  }
}
