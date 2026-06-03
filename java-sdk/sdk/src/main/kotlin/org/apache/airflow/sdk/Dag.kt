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

import kotlin.Throws

/**
 * A collection of tasks with directional dependencies.
 *
 * Create a [Dag] directly and register tasks with [addTask].
 *
 * The [Builder.Dag] annotation should generally be preferred in user code,
 * where the annotation processor generates the wiring for you. Only use this
 * class directly if you need to do low-level plumbing.
 *
 * @param id Dag identifier. Must contain only ASCII alphanumeric characters,
 *    dashes, dots, or underscores; must be unique within a [Bundle].
 *
 * @see Builder.Dag
 */
class Dag(
  val id: String, // TODO: charset check?
) {
  internal var tasks = mutableMapOf<String, Class<out Task>>()

  /**
   * Registers a task with this Dag.
   *
   * The class must have a public no-argument constructor and implement [Task].
   * Task IDs must be unique within a Dag.
   *
   * @param id Task identifier, unique within this Dag.
   * @param definition Class that implements [Task]. Must have a public no-arg
   *    constructor.
   * @return This Dag, for chaining.
   * @throws IllegalArgumentException if a task already exists in the Dag with
   *    the same ID.
   */
  fun addTask(
    id: String,
    definition: Class<out Task>,
  ): Dag {
    require(tasks.putIfAbsent(id, definition) == null) {
      "Tasks in Dag have duplicate ID: $id"
    }
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
