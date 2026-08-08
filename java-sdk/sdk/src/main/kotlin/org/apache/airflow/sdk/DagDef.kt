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

import org.apache.airflow.sdk.internal.SchemaFields
import org.apache.airflow.sdk.internal.checkConfigValue
import kotlin.Throws

/**
 * A collection of tasks with directional dependencies.
 *
 * Create a [DagDef] directly and register [TaskDef]s with [addTask].
 *
 * The [Builder.Dag] annotation should generally be preferred in user code,
 * where the annotation processor generates the wiring for you. Only use this
 * class directly if you need to do low-level plumbing:
 *
 * ```java
 * var extract = new TaskDef("extract", Extract.class).config("retries", 2);
 * var load = new TaskDef("load", Load.class).dependsOn(extract);
 *
 * var dag = new DagDef("java_etl")
 *     .config("schedule", "@daily")
 *     .addTask(extract)
 *     .addTask(load);
 * ```
 *
 * @param id Dag identifier. Must contain only ASCII alphanumeric characters,
 *    dashes, dots, or underscores; must be unique within a [Bundle].
 *
 * @see Builder.Dag
 */
class DagDef(
  val id: String, // TODO: charset check?
) {
  internal val tasks = linkedMapOf<String, TaskDef>()
  internal val dagConfig = linkedMapOf<String, Any>()

  /**
   * Sets one Dag-level configuration value.
   *
   * Keys are the Dag serialization schema property names (for example
   * `"schedule"`, `"description"`, `"tags"`, `"catchup"`); unknown keys and
   * mismatched value types are rejected immediately, so mistakes surface at
   * Dag-parse time.
   *
   * @param key Dag serialization schema property name.
   * @param value Value matching the key's schema type. Durations take
   *    [java.time.Duration], date-times [java.time.OffsetDateTime] or
   *    [java.time.Instant], string arrays any `Iterable` of `String`.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): DagDef {
    dagConfig[key] = checkConfigValue("Dag", SchemaFields.DAG, key, value)
    return this
  }

  /**
   * Registers a task with this Dag.
   *
   * A [TaskDef] belongs to at most one [DagDef]; registering the same instance
   * with a second Dag, or twice with the same one, fails. Task IDs must be
   * unique within a Dag. Upstream tasks referenced via [TaskDef.dependsOn]
   * must be registered with the same Dag by the time it is added to a
   * [Bundle].
   *
   * @param task Task definition to register.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if the task already belongs to a Dag or a
   *    task with the same ID is already registered.
   */
  fun addTask(task: TaskDef): DagDef {
    require(task.owner == null) {
      "Task '${task.id}' already belongs to Dag '${task.owner?.id}'"
    }
    require(tasks.putIfAbsent(task.id, task) == null) {
      "Tasks in Dag have duplicate ID: ${task.id}"
    }
    task.owner = this
    return this
  }

  /**
   * Registers a task with this Dag, adding [upstreams] as its dependencies.
   *
   * Equivalent to `addTask(task.dependsOn(...))`; see [addTask] and
   * [TaskDef.dependsOn].
   *
   * @param task Task definition to register.
   * @param upstreams Tasks that [task] depends on (its upstream tasks).
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if the task already belongs to a Dag or a
   *    task with the same ID is already registered.
   */
  fun addTask(
    task: TaskDef,
    upstreams: List<TaskDef>,
  ): DagDef {
    upstreams.forEach { task.dependsOn(it) }
    return addTask(task)
  }
}

/**
 * One task definition: its ID, the class that implements it, its upstream
 * dependencies, and its task-level configuration.
 *
 * ```java
 * var extract = new TaskDef("extract", Extract.class).config("retries", 2);
 * var load = new TaskDef("load", Load.class).dependsOn(extract);
 * ```
 *
 * @param id Task identifier, unique within a [DagDef].
 * @param definition Class that implements [Task]. Must have a public no-arg
 *    constructor.
 *
 * @see Builder.Task
 */
class TaskDef(
  val id: String,
  val definition: Class<out Task>,
) {
  internal val configValues = linkedMapOf<String, Any>()
  internal val upstreams = linkedSetOf<TaskDef>()
  internal val inputs = mutableListOf<In<*>>()
  internal var owner: DagDef? = null

  /**
   * Sets one task-level configuration value.
   *
   * Keys are the Dag serialization schema property names (for example
   * `"retries"`, `"queue"`, `"retry_delay"`); unknown keys and mismatched
   * value types are rejected immediately, so mistakes surface at Dag-parse
   * time.
   *
   * @param key Dag serialization schema property name, e.g. `"retries"`.
   * @param value Value matching the key's schema type. Durations take
   *    [java.time.Duration], date-times [java.time.OffsetDateTime] or
   *    [java.time.Instant].
   * @return This task definition, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): TaskDef {
    configValues[key] = checkConfigValue("task", SchemaFields.TASK, key, value)
    return this
  }

  /**
   * Declares that this task runs after [upstreams].
   *
   * Referenced tasks must be registered with the same [DagDef] as this task by
   * the time it is added to a [Bundle].
   *
   * @param upstreams Tasks this task depends on.
   * @return This task definition, for chaining.
   */
  fun dependsOn(vararg upstreams: TaskDef): TaskDef {
    this.upstreams += upstreams
    return this
  }
}

/**
 * A single unit of work executed by Airflow.
 *
 * Prefer using the [Builder.Task] annotation with [Builder.Dag] to have the
 * annotation processor generate an implementation for you. Only use this
 * interface if you need to do low-level plumbing.
 *
 * Implement this interface to define task logic. Airflow instantiates the class
 * via its no-argument constructor, then calls [execute] once per task-instance
 * run.
 *
 * @see Builder.Dag
 * @see Builder.Task
 */
interface Task {
  /**
   * Executes this task.
   *
   * Any exception thrown marks the task instance as failed. Use [client] to
   * read connections, variables, pull XComs, or to push an XCom for downstream
   * tasks.
   *
   * @param context Runtime context for the current execution workload.
   * @param client Client for Airflow API calls scoped to this exxecution.
   * @throws Exception on failure; the task instance is marked failed.
   */
  @Throws(Exception::class)
  fun execute(
    context: Context,
    client: Client,
  )
}
