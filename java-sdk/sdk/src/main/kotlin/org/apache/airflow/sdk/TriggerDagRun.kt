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

import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

/** A Dag run state, as `allowed_states` and `failed_states` name one. */
internal val DAG_RUN_STATES = listOf("queued", "running", "success", "failed")

/** Shape of one `TriggerDagRun` setting, for [TriggerDagRun.config] to check a value against. */
private enum class Setting {
  STRING,
  BOOLEAN,
  DURATION,
  DATETIME,
  STATES,
  CONF,
}

/**
 * Settings named as `TriggerDagRunOperator` names its parameters. The Dag ID
 * is the constructor argument, so `trigger_dag_id` is not among them.
 */
private val SETTINGS =
  linkedMapOf(
    "trigger_run_id" to Setting.STRING,
    "conf" to Setting.CONF,
    "logical_date" to Setting.DATETIME,
    "run_after" to Setting.DATETIME,
    "reset_dag_run" to Setting.BOOLEAN,
    "wait_for_completion" to Setting.BOOLEAN,
    "poke_interval" to Setting.DURATION,
    "allowed_states" to Setting.STATES,
    "failed_states" to Setting.STATES,
    "skip_when_already_exists" to Setting.BOOLEAN,
    "fail_when_dag_is_paused" to Setting.BOOLEAN,
    "note" to Setting.STRING,
    "deferrable" to Setting.BOOLEAN,
  )

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
 * The task runs no Java code, takes no arguments and pushes no result other
 * than the triggered run's ID. It renders no templates, so a value such as
 * `"{{ ds }}"` reaches the new run unchanged.
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
   * | `trigger_run_id` | `String`; generated from the trigger time when unset |
   * | `conf` | `Map` with string keys and JSON values |
   * | `logical_date` | [OffsetDateTime] or [Instant]; the trigger time when unset |
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
    val setting =
      requireNotNull(SETTINGS[key]) {
        if (key == "trigger_dag_id") {
          "The Dag to trigger is the TriggerDagRun argument, not the config key 'trigger_dag_id'"
        } else {
          "Unknown TriggerDagRun config key: '$key'"
        }
      }
    requireNotNull(value) { "Value for TriggerDagRun config key '$key' must not be null" }
    settings[key] = check(key, setting, value)
    return this
  }

  private fun check(
    key: String,
    setting: Setting,
    value: Any,
  ): Any {
    fun mismatch(expected: String): Nothing =
      throw IllegalArgumentException(
        "Value for TriggerDagRun config key '$key' must be $expected, got: ${value.javaClass.name}",
      )
    return when (setting) {
      Setting.STRING -> value as? String ?: mismatch("a String")
      Setting.BOOLEAN -> value as? Boolean ?: mismatch("a Boolean")
      Setting.DATETIME ->
        when (value) {
          is OffsetDateTime, is Instant -> value
          else -> mismatch("a java.time.OffsetDateTime or java.time.Instant")
        }
      Setting.DURATION -> {
        val duration = value as? Duration ?: mismatch("a java.time.Duration")
        // Python's poke_interval is a number of seconds, and the serialized Dag carries it as one.
        require(!duration.isNegative && duration.nano == 0) {
          "Value for TriggerDagRun config key '$key' is $duration; it must be a whole, " +
            "non-negative number of seconds"
        }
        duration
      }
      Setting.STATES -> {
        val states =
          when (value) {
            is Iterable<*> -> value.toList()
            is Array<*> -> value.toList()
            else -> mismatch("an Iterable of Dag run states")
          }
        states.map { state ->
          require(state is String && state in DAG_RUN_STATES) {
            "Value for TriggerDagRun config key '$key' holds $state, which is not a Dag run " +
              "state; use one of ${DAG_RUN_STATES.joinToString()}"
          }
          state as String
        }
      }
      Setting.CONF -> {
        val conf = value as? Map<*, *> ?: mismatch("a Map")
        conf.entries.associate { (confKey, entry) ->
          require(confKey is String) { "The conf of TriggerDagRun has a key that is not a String" }
          confKey to entry
        }
      }
    }
  }
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
