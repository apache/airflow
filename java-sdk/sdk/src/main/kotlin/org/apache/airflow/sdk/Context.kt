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

import org.apache.airflow.sdk.execution.Cancellation
import org.apache.airflow.sdk.execution.Logger
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.internal.bindConf
import org.apache.airflow.sdk.internal.writeConfJson
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit
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
   * Each key goes to the field, setter, record component or `@JsonCreator`
   * parameter whose name matches the key when case and underscores are ignored.
   * A key that matches a field exactly wins over the keys that match the same
   * field only when case and underscores are ignored. Objects nested in the
   * conf follow the same rules, including objects in lists and objects that are
   * map values. The keys of a map field stay as written.
   *
   * Keys that match no field are ignored, because other tasks in the run may
   * read them. A field with no matching key keeps its default value. The conf
   * holds only the values that the run was triggered with. This method does not
   * add the defaults of the Dag's params, so in a scheduled run, where the conf
   * is empty, no field has a matching key.
   *
   * The class needs a no-argument constructor and public fields or setters, so
   * a nested class must be `static`. A record and a class with a builder also
   * work. A class with a `@JsonCreator` constructor works when each parameter
   * of the constructor has `@JsonProperty`, which gives the parameter its name.
   * A record component or a constructor parameter with no matching key gets
   * null. If the component or the parameter has a primitive type, this method
   * throws instead. A scheduled run has an empty conf, so a task that reads the
   * conf into a record with a primitive component fails in a scheduled run. In
   * Kotlin, declare the fields with `@JvmField` so that a property such as
   * `isEnabled` keeps its name.
   *
   * To set a field from a key that no Java name can match, such as
   * `target-table`, annotate the field with [ArgName]. The field then takes
   * only that key, as written. You can also read the value of the key from
   * [conf]. Jackson's `@JsonProperty` also sets the key of a field, but other
   * keys still match the name in `@JsonProperty` when case and underscores are
   * ignored. Do not combine `@JsonProperty` with [ArgName] on one field. The
   * SDK keeps Jackson off your compile classpath, so to use a Jackson
   * annotation, add the library that contains the annotation to your build.
   * Annotations such as `@JsonProperty`, `@JsonCreator` and `@JsonAlias` are in
   * `com.fasterxml.jackson.core:jackson-annotations`. `@JsonDeserialize` is in
   * `com.fasterxml.jackson.core:jackson-databind`.
   *
   * An `OffsetDateTime` or `ZonedDateTime` field keeps the offset in the value
   * instead of moving the date-time to UTC. An integer field drops any fraction,
   * so `5.9` reads as `5` in an `int` field.
   *
   * Keys match this way wherever Jackson reads an object field by field, for
   * example in a class that `@JsonTypeInfo` or `@JsonDeserialize(as = ...)`
   * picks, or in the class of a `@JsonUnwrapped` field. For a builder that
   * `@JsonDeserialize(builder = ...)` names, keys match the names that the
   * builder takes. A key that equals a name in `@JsonAlias` counts as an exact
   * match. Two limits apply to `@JsonTypeInfo`. First, the key that holds the
   * type id must equal the name of the type id. Second, with
   * `use = JsonTypeInfo.Id.DEDUCTION`, Jackson picks the class from the keys as
   * written. A deserializer that `@JsonDeserialize` sets on a class or on a
   * field gets the keys of an object as written. So does a deserializer that a
   * Jackson module on the classpath registers. A converter that
   * `@JsonDeserialize` sets and a delegating `@JsonCreator` also get the keys
   * as written when they take a `Map` or a `JsonNode`. When a converter or a
   * delegating `@JsonCreator` takes a class with fields, keys match the fields
   * of that class.
   *
   * In a class with `@JsonIgnoreProperties(ignoreUnknown = true)`, or with a
   * `@JsonUnwrapped` field whose type is a class with fields, Jackson drops two
   * kinds of key without an error. One is a key that matches no field. The
   * other is a key that matches a field when another key has the field's exact
   * name. A class with a `@JsonAnySetter` method receives both kinds of key in
   * that method. Jackson handles the two kinds of key this way even in a task
   * argument or a typed `getXCom`, which otherwise fail on such keys.
   *
   * This method throws [IllegalArgumentException] in these cases:
   *
   * * A value, such as an empty string or `3000000000` for an `int` field,
   *   cannot be converted to the type of its field.
   * * A key holds null for a primitive field, a list holds null for a primitive
   *   array such as `int[]`, or a primitive record component or constructor
   *   parameter has no matching key. To let a field, a component or a parameter
   *   receive null, declare it with a boxed type, such as `Integer`. This
   *   method does not throw when Jackson handles the null on its own. Jackson
   *   handles the null with `@JsonSetter(nulls = Nulls.AS_EMPTY)`, with
   *   `Nulls.SKIP` on a field or a setter, and with a deserializer of the field
   *   that gives a value for null.
   * * Two keys, such as `target_table` and `TARGET_TABLE`, match the same field
   *   after case and underscores are ignored, but neither key matches the field
   *   name exactly.
   * * The class has two fields whose names differ only in case or underscores,
   *   such as `targetTable` and `target_table`. For a field with [ArgName], the
   *   name in [ArgName] counts as the name of the field. For a field of a class
   *   that a `@JsonUnwrapped` field reads, the name of the field includes the
   *   prefix and the suffix from `@JsonUnwrapped`. This method also throws when
   *   the name in [ArgName] on one field equals the name of another field in
   *   the same class or in a superclass. When a field of the class and a field
   *   in the class of a `@JsonUnwrapped` field have equal names, this method
   *   does not throw. The key then goes only to the field of the class itself.
   *   This method checks the class that it reads the conf into even when the
   *   conf is empty, so this method throws in a scheduled run too. The class of
   *   a nested object is checked only when the conf has that nested object.
   *
   * @param type Class to read the conf into.
   * @throws IllegalArgumentException in the cases listed above.
   */
  fun <T : Any> getConf(type: Class<T>): T = bindConf(conf, type)

  /**
   * Reads [conf] into a new instance of a generic type, which a `Class` cannot
   * express. Keys match fields in the same way as in the overload that takes a
   * `Class`.
   *
   * ```java
   * Map<String, TableConf> tables = context.dagRun.getConf(new TypeRef<Map<String, TableConf>>() {});
   * ```
   *
   * @param type Type to read the conf into.
   * @throws IllegalArgumentException in the same cases as the overload that takes
   *    a `Class`.
   */
  fun <T : Any> getConf(type: TypeRef<T>): T = bindConf(conf, type.type)

  /**
   * Writes [conf] as a JSON string, for a task that parses the conf with its own
   * JSON library.
   *
   * Airflow stores the conf as parsed values when it creates the run, so this
   * string is not the text that the run was triggered with. A value of `1.10`
   * comes back as `1.1`, for example. The keys follow the order of [conf]. On
   * PostgreSQL and MySQL, that order can differ from the order in the trigger
   * request, because both databases reorder the keys of a JSON object.
   */
  fun getConfJson(): String = writeConfJson(conf)
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

  // Only from() sets this field. A context made in any other way, including with copy(), keeps it null.
  private var cancellation: Cancellation? = null

  /**
   * Whether Airflow has asked this task to stop.
   *
   * Airflow asks a running task to stop when, for example, a user marks its task
   * instance as failed. The supervisor, which is the Airflow process that started
   * the JVM, then sends the JVM SIGTERM. If the JVM is still running after a
   * grace period, the supervisor sends SIGKILL.
   *
   * Only a [StoppableTask] can call this method, because the SDK catches the stop
   * signal only for that type of task. For any other task, the JVM handles the
   * signal as it would without the SDK: by default the JVM runs its shutdown hooks
   * and exits. Such a task could never see a stop request, so this method throws
   * instead of returning false. If the exception fails the task, Airflow does not
   * retry the task, because a retry would fail the same way.
   *
   * The context that the SDK passes to the task keeps seeing the stop request
   * after the task returns. A context made in any other way never sees a stop
   * request, and this method returns false for it. That includes a context that
   * a unit test makes to call a task directly, and a context made with `copy()`.
   *
   * @throws IllegalStateException if the running task is not a [StoppableTask].
   */
  fun isCancelled(): Boolean = cancellation?.check() ?: false

  /**
   * Waits until Airflow asks this task to stop, or until [timeout] passes.
   *
   * Use this method in place of [Thread.sleep] in a polling loop, so the task
   * stops waiting as soon as Airflow asks it to stop. See [isCancelled] for when
   * Airflow asks a task to stop, and for which tasks can call this method.
   *
   * On a context that never sees a stop request, as described in [isCancelled],
   * this method waits for the whole [timeout] and returns false.
   *
   * @param timeout How long to wait at most.
   * @return `true` if Airflow asked the task to stop, `false` if [timeout]
   *    passed first.
   * @throws IllegalStateException if the running task is not a [StoppableTask].
   * @throws InterruptedException if the waiting thread is interrupted.
   */
  @Throws(InterruptedException::class)
  fun awaitCancellation(timeout: Duration): Boolean {
    val cancellation = cancellation
    if (cancellation == null) {
      TimeUnit.NANOSECONDS.sleep(TimeUnit.NANOSECONDS.convert(timeout))
      return false
    }
    return cancellation.await(timeout)
  }

  internal companion object {
    fun from(
      request: StartupDetails,
      taskDef: TaskDef? = null,
      cancellation: Cancellation? = null,
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
      ).also {
        it.taskDef = taskDef
        it.cancellation = cancellation
      }
  }
}
