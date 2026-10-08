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

// Mirrors TriggerDagRunOperator.execute and execute_complete in the standard
// provider, as the Go and TypeScript SDK runtimes do.

package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.TriggerDagRun
import org.apache.airflow.sdk.execution.comm.DeferTask
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TaskState
import java.security.SecureRandom
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.random.asKotlinRandom

/** `DagStateTrigger`, the Python triggerer runs the wait of a deferred task with. */
private const val DAG_STATE_TRIGGER = "airflow.providers.standard.triggers.external_task.DagStateTrigger"

/** `TriggerDagRunLink().xcom_key`: the XCom the "Triggered DAG" extra link reads. */
private const val LINK_XCOM_KEY = "_link_TriggerDagRunLink"
private const val RUN_ID_XCOM_KEY = "trigger_run_id"

/** `TRIGGER_FAIL_REPR`: the `next_method` a failed or timed-out trigger resumes with. */
private const val TRIGGER_FAIL = "__fail__"
private const val EXECUTE_COMPLETE = "execute_complete"

private val DEFAULT_POKE_INTERVAL = Duration.ofSeconds(60)

internal object TriggerRunner {
  private val logger = Logger(TriggerRunner::class)

  /**
   * Runs a task declared from a [TriggerDagRun], and reports the state it
   * leaves the task instance in.
   *
   * A task resumed by the triggerer reads its outcome from the event the
   * trigger fired with, rather than triggering a second run.
   */
  fun run(
    trigger: TriggerDagRun,
    request: StartupDetails,
    client: Client,
    env: (String) -> String? = System::getenv,
    sleep: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
    now: () -> OffsetDateTime = { OffsetDateTime.now(ZoneOffset.UTC) },
    randomSuffix: () -> String = ::randomRunIdSuffix,
  ): Any {
    val nextMethod = request.tiContext?.nextMethod as String?
    if (nextMethod != null) return resume(trigger, request, nextMethod)

    val runAfter = asOffsetDateTime(trigger.settings["run_after"])
    // `DagRun.generate_run_id`: a run the task gave only a run_after has no logical date, and is
    // named after run_after plus a random suffix. Otherwise the logical date names it, and is the
    // trigger time when the task set none.
    val configuredLogicalDate = asOffsetDateTime(trigger.settings["logical_date"])
    val logicalDate =
      when {
        configuredLogicalDate != null -> configuredLogicalDate
        runAfter == null -> now()
        else -> null
      }
    val namesRun = runAfter ?: logicalDate!!
    val runId =
      trigger.setting<String>("trigger_run_id")
        ?: ("manual__${pythonIsoformat(namesRun)}" + if (logicalDate == null) "_${randomSuffix()}" else "")

    if (trigger.flag("fail_when_dag_is_paused") && client.impl.isDagPaused(trigger.dagId)) {
      return failed(request, "Dag ${trigger.dagId} is paused")
    }

    logger.info("Triggering Dag Run.", mapOf("trigger_dag_id" to trigger.dagId))
    client.setXCom(LINK_XCOM_KEY, dagRunUrl(env, trigger.dagId, runId))
    val alreadyExists =
      client.impl.triggerDagRun(
        dagId = trigger.dagId,
        runId = runId,
        logicalDate = logicalDate,
        runAfter = runAfter,
        conf = trigger.setting("conf"),
        resetDagRun = trigger.flag("reset_dag_run"),
        note = trigger.setting("note"),
      )
    if (alreadyExists) {
      if (trigger.flag("skip_when_already_exists")) {
        logger.info(
          "Dag Run already exists, skipping task as skip_when_already_exists is set to True.",
          mapOf("dag_id" to trigger.dagId),
        )
        return TaskResult.of(TaskState.State.SKIPPED)
      }
      logger.error("Dag Run already exists, marking task as failed.", mapOf("dag_id" to trigger.dagId))
      return TaskResult.of(TaskState.State.FAILED)
    }
    logger.info("Dag Run triggered successfully.", mapOf("trigger_dag_id" to trigger.dagId))
    client.setXCom(RUN_ID_XCOM_KEY, runId)

    if (!trigger.flag("wait_for_completion")) {
      if (deferrable(trigger, env)) {
        logger.info(
          "Ignoring deferrable=True because wait_for_completion=False. " +
            "Task will complete immediately without waiting for the triggered DAG run.",
          mapOf("trigger_dag_id" to trigger.dagId),
        )
      }
      return TaskResult.success()
    }

    val pokeInterval = trigger.setting<Duration>("poke_interval") ?: DEFAULT_POKE_INTERVAL
    if (deferrable(trigger, env)) {
      logger.info("Pausing task as DEFERRED.", mapOf("trigger_dag_id" to trigger.dagId, "run_id" to runId))
      return defer(trigger, request, runId, pokeInterval, env)
    }

    while (true) {
      logger.info(
        "Waiting for dag run to complete execution in allowed state.",
        mapOf("dag_id" to trigger.dagId, "run_id" to runId, "allowed_state" to allowedStates(trigger)),
      )
      sleep(pokeInterval)
      val state = client.impl.getDagRunState(trigger.dagId, runId)
      if (state in failedStates(trigger)) {
        logger.error("DagRun finished with failed state.", mapOf("dag_id" to trigger.dagId, "state" to state))
        return failed(request, "${trigger.dagId} failed with failed state $state")
      }
      if (state in allowedStates(trigger)) {
        logger.info("DagRun finished with allowed state.", mapOf("dag_id" to trigger.dagId, "state" to state))
        return TaskResult.success()
      }
      logger.debug(
        "DagRun not yet in allowed or failed state.",
        mapOf("dag_id" to trigger.dagId, "state" to state),
      )
    }
  }

  /** `DagStateTrigger.serialize()`, key for key, so the Python triggerer runs the wait. */
  private fun defer(
    trigger: TriggerDagRun,
    request: StartupDetails,
    runId: String,
    pokeInterval: Duration,
    env: (String) -> String?,
  ): DeferTask =
    DeferTask().also {
      it.state = "deferred"
      it.classpath = DAG_STATE_TRIGGER
      it.triggerKwargs =
        mapOf(
          "dag_id" to trigger.dagId,
          "states" to allowedStates(trigger) + failedStates(trigger),
          "poll_interval" to pokeInterval.seconds.toInt(),
          "run_ids" to listOf(runId),
          "execution_dates" to null,
        )
      it.triggerTimeout = null
      // `_defer_task` hands the trigger the task's queue only when triggerer queues are enabled.
      it.queue = if (booleanEnv(env, "AIRFLOW__TRIGGERER__QUEUES_ENABLED", false)) request.ti.queue else null
      it.nextMethod = EXECUTE_COMPLETE
      it.nextKwargs = emptyMap<String, Any?>()
    }

  /** `BaseOperator.resume_execution` for this task: `__fail__` or `execute_complete`. */
  private fun resume(
    trigger: TriggerDagRun,
    request: StartupDetails,
    nextMethod: String,
  ): Any {
    val kwargs = request.tiContext?.nextKwargs as? Map<*, *> ?: emptyMap<Any?, Any?>()
    if (nextMethod == TRIGGER_FAIL) {
      (kwargs["traceback"] as? List<*>)?.let { logger.error("Trigger failed:\n${it.joinToString("\n")}") }
      return failed(request, (kwargs["error"] ?: "Unknown").toString())
    }
    if (nextMethod != EXECUTE_COMPLETE) {
      return failed(request, "Task cannot resume with next_method \"$nextMethod\"")
    }
    val event =
      decodeEvent(kwargs["event"])
        ?: return failed(request, "Task resumed with an event it cannot read: ${kwargs["event"]}")
    val runIds =
      event["run_ids"] as? List<*> ?: return failed(
        request,
        "Task resumed with an event it cannot read: ${kwargs["event"]}",
      )
    val failedRunIds =
      runIds.filter { runId ->
        val state = event[runId.toString()].toString()
        if (state in failedStates(trigger)) return@filter true
        if (state in allowedStates(trigger)) {
          logger.info(
            "Triggered Dag run finished with allowed state.",
            mapOf("dag_id" to trigger.dagId, "state" to state, "run_id" to runId.toString()),
          )
        }
        false
      }
    if (failedRunIds.isNotEmpty()) {
      return failed(
        request,
        "${trigger.dagId} failed with failed states ${failedStates(trigger)} for run_ids $failedRunIds",
      )
    }
    return TaskResult.success()
  }

  /**
   * The payload `DagStateTrigger` fired with: `(classpath, data)`. The
   * triggerer stores it with serde, which encodes a tuple as
   * `{__classname__, __data__}`.
   */
  private fun decodeEvent(event: Any?): Map<String, Any?>? {
    val pair =
      if (event is Map<*, *> && event["__classname__"] == "builtins.tuple") event["__data__"] else event
    val items = pair as? List<*> ?: return null
    if (items.size != 2) return null
    val data = items[1] as? Map<*, *> ?: return null
    return data.entries.associate { (key, value) -> key.toString() to value }
  }

  private fun failed(
    request: StartupDetails,
    message: String,
  ): Any {
    logger.error(message)
    return TaskResult.failure(request.tiContext?.shouldRetry == true)
  }

  private fun allowedStates(trigger: TriggerDagRun): List<String> =
    trigger.setting<List<String>>("allowed_states")?.takeIf { it.isNotEmpty() } ?: listOf("success")

  // An empty failed_states that was set means no state fails the task, so it is kept as written.
  private fun failedStates(trigger: TriggerDagRun): List<String> = trigger.setting<List<String>>("failed_states") ?: listOf("failed")

  private fun deferrable(
    trigger: TriggerDagRun,
    env: (String) -> String?,
  ): Boolean =
    trigger.setting<Boolean>("deferrable")
      ?: booleanEnv(env, "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE", false)
}

@Suppress("UNCHECKED_CAST")
private fun <T> TriggerDagRun.setting(key: String): T? = settings[key] as T?

private fun TriggerDagRun.flag(key: String): Boolean = settings[key] == true

/**
 * An Airflow boolean option from the environment. It takes what Go's `strconv.ParseBool` takes
 * rather than every spelling `configparser` accepts, so `yes` and `on` are rejected rather than
 * read as true.
 */
internal fun booleanEnv(
  env: (String) -> String?,
  name: String,
  fallback: Boolean,
): Boolean {
  val raw = env(name) ?: return fallback
  return when (raw.trim().lowercase()) {
    "t", "true", "1" -> true
    "f", "false", "0" -> false
    else -> throw IllegalArgumentException("$name is \"$raw\", which is not a boolean; use true or false")
  }
}

/** `build_airflow_dagrun_url`, on `[api] base_url` from the environment. */
internal fun dagRunUrl(
  env: (String) -> String?,
  dagId: String,
  runId: String,
): String {
  val base = env("AIRFLOW__API__BASE_URL")?.takeIf { it.isNotEmpty() } ?: "/"
  return "${base.trimEnd('/')}/dags/$dagId/runs/$runId"
}

/** The eight characters `get_random_string` adds to the ID of a run with no logical date. */
private const val RUN_ID_SUFFIX_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

private val RUN_ID_RANDOM = SecureRandom().asKotlinRandom()

internal fun randomRunIdSuffix(): String = (1..8).map { RUN_ID_SUFFIX_CHARS.random(RUN_ID_RANDOM) }.joinToString("")

/** `datetime.isoformat()` of a UTC instant, which is how Python spells a run ID's date. */
internal fun pythonIsoformat(moment: OffsetDateTime): String {
  val utc = moment.withOffsetSameInstant(ZoneOffset.UTC)
  val micros = utc.nano / 1000
  val seconds = utc.format(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss"))
  return if (micros == 0) "$seconds+00:00" else "$seconds.%06d+00:00".format(micros)
}

/** Reads a setting that may be stored as either temporal type. */
internal fun asOffsetDateTime(value: Any?): OffsetDateTime? =
  when (value) {
    null -> null
    is OffsetDateTime -> value
    is Instant -> value.atOffset(ZoneOffset.UTC)
    else -> throw IllegalArgumentException("Not a date-time: $value")
  }
