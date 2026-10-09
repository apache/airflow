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

import org.apache.airflow.sdk.execution.ArgBinding
import org.apache.airflow.sdk.execution.Client
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.decodeArgBindings
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.math.floor

/**
 * A connection registered in Airflow's connection store.
 *
 * @property id Connection ID as configured in Airflow.
 * @property type Connection type (e.g. `"http"`, `"postgres"`), if configured.
 * @property host Hostname, if configured.
 * @property schema Schema or database name, if configured.
 * @property login Username, if configured.
 * @property password Password, if configured.
 * @property port Port number, if configured.
 * @property extra JSON blob of extra connection parameters, if configured.
 */
data class Connection(
  @JvmField val id: String,
  @JvmField val type: String?,
  @JvmField val host: String?,
  @JvmField val schema: String?,
  @JvmField val login: String?,
  @JvmField val password: String?,
  @JvmField val port: Int?,
  @JvmField val extra: Any?,
)

/**
 * Client for Airflow API calls scoped to the current task instance.
 *
 * An instance is provided when a task is being executed. All reads and writes
 * are automatically scoped to the current Dag run and task instance unless you
 * pass explicit IDs.
 */
class Client internal constructor(
  internal val details: StartupDetails,
  internal val impl: Client,
  env: (String) -> String? = System::getenv,
) {
  internal companion object {
    /**
     * Default XCom key used for a task's return value ({@value}).
     */
    const val XCOM_RETURN_KEY = "return_value"
  }

  /**
   * Key-value state scoped to the current task instance.
   *
   * Entries survive retries of the task instance within the same Dag run, so
   * they can carry things like an external job ID across attempts.
   */
  val taskStateStore: TaskStateStore = TaskStateStore(details, impl, env)

  /**
   * Gives the task the state store of each asset, which it looks up by the
   * asset's name or URI.
   *
   * Entries belong to the asset, not to a task instance or a Dag run. A value
   * that one Dag run stores, such as an incremental-load watermark, is still
   * there in later runs:
   *
   * ```java
   * var orders = client.getAssetStateStore().byName("orders");
   * var watermark = (String) orders.get("watermark"); // null until a task sets it
   * orders.set("watermark", "2026-10-01T00:00:00Z");
   * ```
   */
  val assetStateStore: AssetStateStores = AssetStateStores(impl)

  /**
   * Retrieves a connection from the Airflow connection store.
   *
   * @param id Connection ID as configured in Airflow.
   * @return The connection.
   * @throws ApiError if the connection does not exist or the API call fails.
   */
  fun getConnection(id: String): Connection =
    with(impl.getConnection(id)) {
      Connection(
        id = connId,
        type = connType,
        host = host as String?,
        schema = schema as String?,
        login = login as String?,
        password = password as String?,
        // The msgpack decoder yields Long for wire integers, so convert
        // numerically instead of casting.
        port = (port as Number?)?.toInt(),
        extra = extra,
      )
    }

  /**
   * Retrieves an Airflow variable.
   *
   * @param key Variable key.
   * @return The variable value, or `null` if the variable is not set.
   * @throws ApiError if the API call fails.
   */
  fun getVariable(key: String): Any? = impl.getVariable(key).value

  /**
   * Stores an Airflow variable, replacing any existing value.
   *
   * The value is stored as-is. Serialize structured data (for example to
   * JSON) before storing it.
   *
   * @param key Variable key.
   * @param value Value to store.
   * @param description Description of the variable.
   * @throws ApiError if the API call fails.
   */
  @JvmOverloads fun setVariable(
    key: String,
    value: String,
    description: String? = null,
  ) = impl.setVariable(key = key, value = value, description = description)

  /**
   * Deletes an Airflow variable.
   *
   * @param key Variable key.
   * @throws ApiError if the API call fails.
   */
  fun deleteVariable(key: String) = impl.deleteVariable(key)

  /**
   * Reads an XCom value pushed by another task.
   *
   * The current Dag run's [dagId][TaskInstance.dagId] and
   * [runId][TaskInstance.runId] are used by default; override them only when
   * reading across Dags or runs.
   *
   * @param key XCom key to read; defaults to [XCOM_RETURN_KEY].
   * @param dagId Dag that owns the XCom; defaults to the current Dag.
   * @param taskId Task that pushed the XCom.
   * @param runId Run that produced the XCom; defaults to the current run.
   * @param mapIndex Map index of the source task instance.
   * @param includePriorDates If `true`, also search earlier Dag-run dates.
   * @return The XCom value, or `null` if none was pushed.
   * @throws ApiError if the API call fails.
   *
   * If `map_index` is set to `null` against a mapped task, the task's
   * "collective result" is returned. Results from all mapped instances are
   * aggregated into a list, ordered by the map index (ascending). For a
   * non-mapped task, setting `map_index` to `null` is equivalent to `-1`.
   */
  @JvmOverloads fun getXCom(
    key: String = XCOM_RETURN_KEY,
    dagId: String = details.ti.dagId,
    taskId: String,
    runId: String = details.ti.runId,
    mapIndex: Int? = null,
    includePriorDates: Boolean = false,
  ): Any? =
    impl
      .getXCom(
        key = key,
        dagId = dagId,
        taskId = taskId,
        runId = runId,
        mapIndex = mapIndex,
        includePriorDates = includePriorDates,
      ).value

  /**
   * Pushes an XCom value for downstream tasks to read.
   *
   * @param key XCom key; defaults to [XCOM_RETURN_KEY].
   * @param value Value to push. Must be JSON-serializable.
   * @throws ApiError if the API call fails.
   */
  @JvmOverloads fun setXCom(
    key: String = XCOM_RETURN_KEY,
    value: Any,
  ) = impl.setXCom(
    key = key,
    value = value,
    dagId = details.ti.dagId,
    taskId = details.ti.taskId,
    runId = details.ti.runId,
    mapIndex = details.ti.mapIndex ?: -1,
  )

  internal val argBindings: List<ArgBinding> by lazy {
    decodeArgBindings(details.tiContext?.argBindings)
  }

  // A literal binding carries the inline value from the Dag file; an XCom
  // binding pulls the bound upstream task's return-value XCom, honouring the
  // bound map index and element index.
  internal fun resolveBinding(binding: ArgBinding): Any? =
    when (binding) {
      is ArgBinding.Literal -> binding.value
      is ArgBinding.XCom -> {
        val value = getXCom(taskId = binding.taskId, mapIndex = binding.mapIndex.takeIf { it >= 0 })
        binding.elementIndex?.let { elementOf(value, it, binding) } ?: value
      }
    }

  /**
   * Reads the element a binding indexes out of an upstream's list XCom. An
   * upstream that pushed nothing resolves to null like any other unpushed
   * binding, so whether a parameter can be null stays the parameter's own
   * question rather than the call site's.
   */
  private fun elementOf(
    value: Any?,
    index: Int,
    binding: ArgBinding.XCom,
  ): Any? {
    if (value == null) return null
    val bound = "Argument '${binding.name}' binds element $index of task '${binding.taskId}'"
    check(value is List<*>) { "$bound, but its XCom is not a list" }
    check(index in value.indices) { "$bound, but its XCom holds only ${value.size} element(s)" }
    return value[index]
  }
}

/**
 * Key-value state scoped to one task instance, shared across its retries
 * within the same Dag run.
 *
 * Values must be JSON-serializable. Every key has an expiry: by default the
 * deployment's `[state_store] default_retention_days`, which the coordinator
 * passes to the JVM as `AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS`; [set]
 * also takes an explicit retention, or [NEVER_EXPIRE] for a key that garbage
 * collection skips.
 *
 * Values are stored in the metadata database as-is; the `[workers]
 * state_store_backend` used by Python tasks is not applied here.
 */
class TaskStateStore internal constructor(
  private val details: StartupDetails,
  private val impl: Client,
  private val env: (String) -> String?,
) {
  companion object {
    /**
     * Pass as the retention of [set] to store a key that never expires and is
     * skipped by Airflow's periodic garbage collection.
     */
    @JvmField val NEVER_EXPIRE: Duration = ChronoUnit.FOREVER.duration

    internal const val DEFAULT_RETENTION_DAYS_ENV = "AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS"
  }

  /**
   * Reads the value stored under [key].
   *
   * @return The stored value, or `null` if the key is not set.
   * @throws ApiError if the API call fails.
   */
  fun get(key: String): Any? = impl.getTaskStateStore(details.ti.id, key)?.value

  /**
   * Stores [value] under [key], replacing any existing value.
   *
   * @param key State key.
   * @param value Value to store. Must be JSON-serializable.
   * @param retention How long to keep the key. Must be positive, or
   *   [NEVER_EXPIRE]; `null` uses `[state_store] default_retention_days`.
   * @throws IllegalArgumentException if [retention] is zero or negative, or the
   *   default retention from the environment is not a non-negative integer.
   * @throws IllegalStateException if [retention] is `null` and the coordinator
   *   did not pass `[state_store] default_retention_days` to the JVM.
   * @throws ApiError if the API call fails.
   */
  @JvmOverloads fun set(
    key: String,
    value: Any,
    retention: Duration? = null,
  ) {
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val expiresAt =
      when {
        retention == null -> resolveDefaultExpiry(now)
        retention == NEVER_EXPIRE -> null
        retention.isNegative || retention.isZero ->
          throw IllegalArgumentException(
            "Task state retention must be positive or TaskStateStore.NEVER_EXPIRE, got $retention for key '$key'",
          )
        else -> now.plus(retention)
      }
    // TODO: warn when the serialized value exceeds [state_store] max_value_storage_bytes once the
    //   coordinator passes it to the JVM, as the Python accessor does.
    impl.setTaskStateStore(tiId = details.ti.id, key = key, value = value, expiresAt = expiresAt)
  }

  /**
   * Deletes the value stored under [key]. Does nothing if the key is not set.
   *
   * @throws ApiError if the API call fails.
   */
  fun delete(key: String) = impl.deleteTaskStateStore(details.ti.id, key)

  /**
   * Deletes every key stored for this task instance.
   *
   * @throws ApiError if the API call fails.
   */
  fun clear() = impl.clearTaskStateStore(details.ti.id)

  private fun resolveDefaultExpiry(now: OffsetDateTime): OffsetDateTime? {
    val raw =
      env(DEFAULT_RETENTION_DAYS_ENV)
        ?: throw IllegalStateException(
          "$DEFAULT_RETENTION_DAYS_ENV is not set, so the default retention is unknown. The coordinator passes " +
            "[state_store] default_retention_days to the JVM; pass a retention or TaskStateStore.NEVER_EXPIRE " +
            "to set the expiry explicitly.",
        )
    val days = parseRetentionDays(raw)
    return if (days == 0) null else now.plusDays(days.toLong())
  }

  // Accepts "7.0" because Python's conf.getint does.
  private fun parseRetentionDays(raw: String): Int {
    val days =
      raw.trim().toIntOrNull()
        ?: raw
          .trim()
          .toDoubleOrNull()
          ?.takeIf { it.isFinite() && it == floor(it) }
          ?.toInt()
        ?: throw IllegalArgumentException(
          "Failed to convert value to int. Please check 'default_retention_days' key in 'state_store' section. " +
            "Current value: '$raw'",
        )
    require(days >= 0) { "[state_store] default_retention_days must be >= 0, got $days. Set to 0 to disable expiry." }
    return days
  }
}

/**
 * Thrown when a task's input resolves to nothing where a value is required —
 * a data parameter or a [TaskInput] field with a primitive type.
 */
class MissingXComException(
  message: String,
) : IllegalStateException(message) {
  constructor(
    taskId: String,
    paramName: String,
  ) : this(
    "Task parameter '$paramName' requires an XCom from task '$taskId', but none was pushed. " +
      "This parameter has a primitive type that cannot be null; declare it with a boxed type " +
      "(e.g. Integer instead of int) to receive null.",
  )
}
