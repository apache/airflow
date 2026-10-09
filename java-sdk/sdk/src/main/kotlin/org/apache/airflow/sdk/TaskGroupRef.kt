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

import org.apache.airflow.sdk.internal.deriveTaskId

/**
 * A group of tasks in a Dag, shown in the Airflow UI as one node that expands:
 * Python's `TaskGroup`.
 *
 * Everything declared in a group carries the group's ID as a prefix, so task
 * `stage` in group `staging` is the task `staging.stage`. A group can stand
 * at either end of an edge, so a whole group can be ordered against a task or
 * another group:
 *
 * ```java
 * var staging = dag.taskGroup("staging");
 * staging.task("stage", Stage.class);
 * extract.before(staging); // every task staging starts with waits for extract
 * ```
 *
 * As an upstream, a group stands for its leaves, the tasks nothing else in the
 * group runs after; as a downstream, for its roots, the tasks that run after
 * nothing else in the group. What the group holds is read once, rather than at
 * each edge as Python reads it, so a group's edges can be drawn before its
 * tasks are declared. The edges still resolve in the order they were drawn.
 *
 * @property id Group ID, including any enclosing group's prefix.
 */
class TaskGroupRef internal constructor(
  internal val dag: DagDef,
  val id: String,
  internal val parent: TaskGroupRef? = null,
) : Deps.Flow,
  Endpoint {
  /** IDs of the tasks declared directly in this group, in declaration order. */
  internal val taskIds = mutableListOf<String>()

  /** Groups nested directly in this group, in declaration order. */
  internal val children = mutableListOf<TaskGroupRef>()

  /**
   * Creates a task in this group, registers it with the Dag, and hands back
   * its handle.
   *
   * @param id Task ID within this group; the task's ID is `<group ID>.<id>`.
   * @param definition Class that implements [Task]. Must have a public no-arg
   *    constructor.
   * @return The handle representing this task.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   */
  fun <T> task(
    id: String,
    definition: Class<out Task>,
  ): TaskRef<T> {
    val def = TaskDef(qualify(id), definition)
    adopt(def)
    return TaskRef(def)
  }

  /**
   * Declares a task in this group that starts a run of another Dag, as
   * [DagDef.task] does for the Dag.
   *
   * @param id Task ID within this group; the task's ID is `<group ID>.<id>`.
   * @param trigger What to trigger, and how.
   * @return The handle representing this task.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   */
  fun task(
    id: String,
    trigger: TriggerDagRun,
  ): TaskRef<Void> {
    val def = TaskDef(qualify(id), trigger)
    adopt(def)
    return TaskRef(def)
  }

  /**
   * Declares a condition in this group, as [DagDef.If] does for the Dag.
   *
   * @param definition Class that implements [ConditionTask]. Must have a
   *    public no-arg constructor.
   * @return The condition, to name each side on.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   */
  @Suppress("ktlint:standard:function-naming")
  fun If(definition: Class<out ConditionTask>): ConditionRef = If(deriveTaskId(definition), definition)

  /**
   * Declares a condition in this group under the task ID `<group ID>.<id>`.
   *
   * @param id Task ID within this group.
   * @param definition Class that implements [ConditionTask]. Must have a
   *    public no-arg constructor.
   * @return The condition, to name each side on.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   *
   * @see If
   */
  @Suppress("ktlint:standard:function-naming")
  fun If(
    id: String,
    definition: Class<out ConditionTask>,
  ): ConditionRef = ConditionRef.of(task(id, definition))

  /**
   * Declares a switch in this group, as [DagDef.Switch] does for the Dag.
   *
   * @param definition Class that implements [SwitchTask]. Must have a public
   *    no-arg constructor.
   * @return The switch, to list its cases on.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   */
  @Suppress("ktlint:standard:function-naming")
  fun Switch(definition: Class<out SwitchTask>): SwitchRef = Switch(deriveTaskId(definition), definition)

  /**
   * Declares a switch in this group under the task ID `<group ID>.<id>`.
   *
   * @param id Task ID within this group.
   * @param definition Class that implements [SwitchTask]. Must have a public
   *    no-arg constructor.
   * @return The switch, to list its cases on.
   * @throws IllegalArgumentException if the Dag already has a task or task
   *    group with the resulting ID.
   *
   * @see Switch
   */
  @Suppress("ktlint:standard:function-naming")
  fun Switch(
    id: String,
    definition: Class<out SwitchTask>,
  ): SwitchRef = SwitchRef.of(task<Any?>(id, definition))

  /**
   * Nests a task group inside this one.
   *
   * @param id Group ID within this group; the nested group's ID is
   *    `<group ID>.<id>`. Must contain only ASCII letters, digits,
   *    underscores, or dashes.
   * @return The nested group.
   * @throws IllegalArgumentException if [id] is not a valid group ID, or the
   *    Dag already has a task or task group with the resulting ID.
   */
  fun taskGroup(id: String): TaskGroupRef = dag.addGroup(this, id)

  /** Every task in this group, including those in nested groups. */
  override fun nodes(): List<TaskDef> = taskIds.map { dag.tasks.getValue(it) } + children.flatMap { it.nodes() }

  override fun endpoints(): List<Endpoint> = listOf(this)

  override fun before(vararg next: Deps.Flow): TaskGroupRef {
    super.before(*next)
    return this
  }

  override fun after(vararg previous: Deps.Flow): TaskGroupRef {
    super.after(*previous)
    return this
  }

  internal fun qualify(localId: String): String = "$id.$localId"

  /** Registers [def], whose ID already carries this group's prefix, as a task of this group. */
  internal fun adopt(def: TaskDef) {
    dag.addTask(def)
    taskIds += def.id
  }
}
