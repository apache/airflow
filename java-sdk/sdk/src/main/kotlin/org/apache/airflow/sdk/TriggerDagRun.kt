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

import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.FieldType
import org.apache.airflow.sdk.internal.checkConfigValue
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

/**
 * Settings named as `TriggerDagRunOperator` names its parameters. The Dag ID
 * is the constructor argument, so `trigger_dag_id` is not among them.
 */
private val TRIGGER_FIELDS: Map<String, Field> =
  listOf(
    Field("trigger_run_id", "triggerRunId", FieldType.STRING, null),
    Field("conf", "conf", FieldType.JSON_OBJECT, null),
    Field("logical_date", "logicalDate", FieldType.DATETIME, null),
    Field("run_after", "runAfter", FieldType.DATETIME, null),
    Field("reset_dag_run", "resetDagRun", FieldType.BOOLEAN, null),
    Field("wait_for_completion", "waitForCompletion", FieldType.BOOLEAN, null),
    Field("poke_interval", "pokeInterval", FieldType.TIMEDELTA, null),
    Field("allowed_states", "allowedStates", FieldType.DAG_RUN_STATES, null),
    Field("failed_states", "failedStates", FieldType.DAG_RUN_STATES, null),
    Field("skip_when_already_exists", "skipWhenAlreadyExists", FieldType.BOOLEAN, null),
    Field("fail_when_dag_is_paused", "failWhenDagIsPaused", FieldType.BOOLEAN, null),
    Field("note", "note", FieldType.STRING, null),
    Field("deferrable", "deferrable", FieldType.BOOLEAN, null),
  ).associateBy { it.key }

/**
 * A task that starts a run of another Dag, in place of a task class.
 *
 * Pass one to [DagDef.task] and the Java runtime runs it as Airflow's
 * `TriggerDagRunOperator` does, so the Dag needs no Python worker for it:
 *
 * ```java
 * dag.task("trigger_downstream",
 *     new TriggerDagRun("downstream_etl")
 *         .config("wait_for_completion", true)
 *         .config("conf", Map.of("rows", 2)));
 * ```
 *
 * In an annotated Dag, a `@Builder.Task` method returns one instead; the
 * method runs when the Dag is built, not when the task runs.
 *
 * The task runs no Java code and takes no arguments. It pushes the triggered
 * run's ID, and the link the "Triggered DAG" extra link reads. It renders no
 * templates, so a value such as `"{{ ds }}"` reaches the new run unchanged.
 * Like a Java task, it runs on the Dag's `queue` unless the task sets its own.
 *
 * @param dagId `trigger_dag_id`: the Dag to trigger.
 * @throws IllegalArgumentException if [dagId] is empty.
 */
class TriggerDagRun(
  val dagId: String,
) {
  init {
    require(dagId.isNotEmpty()) { "TriggerDagRun needs the ID of the Dag to trigger" }
  }

  internal val settings = linkedMapOf<String, Any>()

  /**
   * Sets one `TriggerDagRunOperator` setting, named as Python names the
   * parameter.
   *
   * | Key | Value |
   * | --- | --- |
   * | `trigger_run_id` | `String`; generated when unset, and for a run with no logical date from `run_after` plus a random eight-character suffix |
   * | `conf` | `Map` with string keys and JSON values |
   * | `logical_date` | [OffsetDateTime] or [Instant]; the trigger time when neither this nor `run_after` is set, and none when only `run_after` is |
   * | `run_after` | [OffsetDateTime] or [Instant] |
   * | `reset_dag_run` | `Boolean`: clear a run with the same ID instead of failing |
   * | `wait_for_completion` | `Boolean`: hold this task open until the run finishes |
   * | `poke_interval` | [Duration], whole seconds; 60 seconds when unset |
   * | `allowed_states` | states that count as success; `success` when unset |
   * | `failed_states` | states that count as failure; `failed` when unset |
   * | `skip_when_already_exists` | `Boolean`: skip rather than fail when the run exists |
   * | `fail_when_dag_is_paused` | `Boolean`: fail rather than trigger a paused Dag |
   * | `note` | `String` recorded against the triggered run |
   * | `deferrable` | `Boolean`: while waiting, defer instead of holding the worker |
   *
   * A `deferrable` left unset follows Airflow's `[operators] default_deferrable`.
   *
   * @param key One of the keys above.
   * @param value Value of the shape that key takes.
   * @return This task, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value does
   *    not match it.
   */
  fun config(
    key: String,
    value: Any?,
  ): TriggerDagRun {
    require(key != "trigger_dag_id") {
      "The Dag to trigger is the TriggerDagRun argument, not the config key 'trigger_dag_id'"
    }
    val checked = checkConfigValue("TriggerDagRun", TRIGGER_FIELDS, key, value)
    settings[key] =
      when (key) {
        "poke_interval" -> {
          val duration = checked as Duration
          // Python's poke_interval is a number of seconds, and the serialized Dag carries it as one.
          require(!duration.isNegative && duration.nano == 0) {
            "Value for TriggerDagRun config key '$key' is $duration; it must be a whole, " +
              "non-negative number of seconds"
          }
          duration
        }
        "conf" -> jsonValue(key, checked)!!
        // Python generates a run ID for an empty one, which the runtime does not.
        "trigger_run_id", "note" -> {
          require((checked as String).isNotEmpty()) {
            "Value for TriggerDagRun config key '$key' must not be empty"
          }
          checked
        }
        else -> checked
      }
    return this
  }

  /** A copy that later calls on this builder cannot change. */
  internal fun snapshot(): TriggerDagRun = TriggerDagRun(dagId).also { it.settings.putAll(settings) }
}

/** [value] as plain JSON, as a fresh structure, rejecting anything that has no JSON form. */
private fun jsonValue(
  key: String,
  value: Any?,
): Any? =
  when (value) {
    null, is String, is Boolean, is Int, is Long, is Short, is Byte -> value
    is Double ->
      value.also {
        require(it.isFinite()) {
          "Value for TriggerDagRun config key '$key' must hold only finite numbers, got: $it"
        }
      }
    is Float -> jsonValue(key, value.toDouble())
    is Collection<*> -> value.map { jsonValue(key, it) }
    is Array<*> -> value.map { jsonValue(key, it) }
    is Map<*, *> ->
      value.entries.associateTo(linkedMapOf<String, Any?>()) { (entryKey, entry) ->
        require(entryKey is String) {
          "Value for TriggerDagRun config key '$key' must have String keys, got: ${entryKey?.javaClass?.name}"
        }
        entryKey to jsonValue(key, entry)
      }
    else ->
      throw IllegalArgumentException(
        "Value for TriggerDagRun config key '$key' must hold only JSON values (null, String, " +
          "Boolean, Number, List, Map), got: ${value.javaClass.name}",
      )
  }

/**
 * Stands in for the class of a task that triggers a Dag run. The runtime runs
 * such a task itself and never instantiates this, so it exists only so that
 * every [TaskDef] names a class.
 */
internal class TriggerDagRunPlaceholder : Task {
  override fun execute(
    context: Context,
    client: Client,
  ): Unit =
    throw IllegalStateException(
      "A task that triggers a Dag run is run by the SDK, so its placeholder is never executed",
    )
}
