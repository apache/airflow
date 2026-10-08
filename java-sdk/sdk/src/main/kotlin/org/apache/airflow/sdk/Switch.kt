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
 * The Airflow task ID of one task of a Dag.
 *
 * A `@Builder.Switch` method names the task it chose with one of these. The
 * generated `<Dag>Builder.TaskIds` holds a constant per task of the Dag, so
 * the choice is checked where it is written rather than when the task runs.
 *
 * @property value The task ID.
 */
class TaskId private constructor(
  val value: String,
) {
  companion object {
    /**
     * @suppress
     *
     * The ID [value] names. Public so a generated `TaskIds` holder can declare
     * its constants; user code names a task through those constants.
     */
    @JvmStatic
    fun of(value: String): TaskId = TaskId(value)
  }

  override fun equals(other: Any?): Boolean = other is TaskId && other.value == value

  override fun hashCode(): Int = value.hashCode()

  override fun toString(): String = value
}

/**
 * A task that chooses one of several tasks to run; every other one is skipped.
 *
 * Register one with [DagDef.Switch], then list what it can choose with
 * [SwitchRef.Case]. The choice is the task's own class, so javac checks it:
 *
 * ```java
 * public class PickPath implements SwitchTask {
 *   @Override
 *   public Class<? extends Task> choose(Context context, Client client) {
 *     return ((Number) client.getXCom("extract")).longValue() > 1000
 *         ? HandleLong.class
 *         : HandleShort.class;
 *   }
 * }
 * ```
 *
 * The SDK runs [choose] and pushes the chosen task's ID as this task's return
 * value, so [execute] is never called.
 *
 * Because a switch names a case by its class, no two of its cases may be
 * registered from the same class; registering the Dag reports that.
 *
 * @see DagDef.Switch
 */
interface SwitchTask : Task {
  /**
   * Chooses the one case that runs.
   *
   * Any exception thrown marks the task instance as failed, and nothing is
   * then pushed or skipped.
   *
   * @param context Runtime context for the current execution workload.
   * @param client Client for Airflow API calls scoped to this execution.
   * @return The class of one of this switch's cases.
   * @throws Exception on failure; the task instance is marked failed.
   */
  @Throws(Exception::class)
  fun choose(
    context: Context,
    client: Client,
  ): Class<out Task>

  /** Never called: the SDK runs a switch through [choose]. */
  override fun execute(
    context: Context,
    client: Client,
  ): Unit =
    throw IllegalStateException(
      "Switch '${javaClass.name}' runs through choose(), so execute() is never called",
    )
}

/**
 * @suppress
 *
 * What a generated `@Builder.Switch` task implements: a switch whose method
 * named the case with a [TaskId]. Public so generated code can implement it;
 * a Dag written against [DagDef.Switch] implements [SwitchTask] instead.
 */
interface TaskIdSwitchTask : Task {
  @Throws(Exception::class)
  fun choose(
    context: Context,
    client: Client,
  ): TaskId

  override fun execute(
    context: Context,
    client: Client,
  ): Unit =
    throw IllegalStateException(
      "Switch '${javaClass.name}' runs through choose(), so execute() is never called",
    )
}

/**
 * A switch registered with a Dag: list the tasks it can choose between.
 *
 * ```java
 * dag.Switch(PickPath.class).Case(handleLong).Case(handleShort);
 * ```
 *
 * A case runs after the switch, so naming it records that edge, as
 * [Deps.Flow.before] would. The switch chooses exactly one case and skips
 * every other, so a task that runs after several cases needs a trigger rule
 * that tolerates a skipped upstream, such as `none_failed_min_one_success`.
 *
 * @see DagDef.Switch
 */
class SwitchRef private constructor(
  private val ref: TaskRef<*>,
  private val cases: SwitchDef,
) : Deps.Flow {
  companion object {
    /**
     * @suppress
     *
     * Marks a registered task as a switch. Public so a generated wiring view
     * can call it; user code reaches a switch through [DagDef.Switch].
     */
    @JvmStatic
    fun of(ref: TaskRef<*>): SwitchRef {
      require(ref.def.decider == null) {
        "Task '${ref.def.id}' already decides what to skip; declare it once"
      }
      val definition = ref.def.definition
      require(
        SwitchTask::class.java.isAssignableFrom(definition) ||
          TaskIdSwitchTask::class.java.isAssignableFrom(definition),
      ) {
        "Task '${ref.def.id}' runs '${definition.name}', which chooses nothing; a switch runs a " +
          "SwitchTask"
      }
      return SwitchRef(ref, SwitchDef().also { ref.def.decider = it })
    }
  }

  /** Airflow task ID of the deciding task. */
  val id: String get() = ref.def.id

  /**
   * Adds a task this switch can choose. Every other case is skipped when the
   * switch chooses this one.
   *
   * @param task Task of the same Dag.
   * @return This switch, for chaining.
   * @throws IllegalArgumentException if the task belongs to another Dag or is
   *    already a case of this switch.
   */
  @Suppress("ktlint:standard:function-naming")
  fun Case(task: TaskRef<*>): SwitchRef {
    cases.add(ref.def, task.def)
    // The case runs after the switch, which is what puts the switch in the
    // serialized Dag as its upstream and lets registration see a cycle that
    // runs through a switch.
    ref.before(task)
    return this
  }

  /**
   * Sets one task-level configuration value on the deciding task.
   *
   * @param key Airflow task setting name.
   * @param value Value matching the key's schema type.
   * @return This switch, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): SwitchRef {
    ref.config(key, value)
    return this
  }

  override fun nodes(): List<TaskDef> = ref.nodes()

  override fun before(vararg next: Deps.Flow): SwitchRef {
    ref.before(*next)
    return this
  }

  override fun after(vararg previous: Deps.Flow): SwitchRef {
    ref.after(*previous)
    return this
  }
}

/** The cases of one switch, as [SwitchRef.Case] listed them. */
internal class SwitchDef : DeciderDef {
  private val options = mutableListOf<TaskDef>()

  override val cases: List<TaskDef> get() = options

  fun add(
    decider: TaskDef,
    case: TaskDef,
  ) {
    requireUnregistered(decider, "Switch")
    requireSameDag(decider, case, "Switch")
    require(options.none { it === case }) {
      "Switch '${decider.id}' already chooses between '${case.id}' and others; name each case once"
    }
    // A SwitchTask names its case by class, so two cases sharing one would be
    // indistinguishable. Checked here rather than at registration so the error
    // points at the second Case(...) call.
    if (SwitchTask::class.java.isAssignableFrom(decider.definition)) {
      options.firstOrNull { it.definition == case.definition }?.let { first ->
        throw IllegalArgumentException(
          "Switch '${decider.id}' cannot choose between '${first.id}' and '${case.id}': both run " +
            "'${case.definition.name}', and a switch names its case by class",
        )
      }
    }
    options += case
  }

  override fun describe(decider: TaskDef): String? =
    if (options.isEmpty()) "Switch '${decider.id}' has no task to choose between; call Case(...)" else null

  override fun decide(
    decider: TaskDef,
    instance: Task,
    context: Context,
    client: Client,
  ): Decision {
    val chosen =
      when (instance) {
        is SwitchTask -> byClass(decider, instance.choose(context, client))
        is TaskIdSwitchTask -> byId(decider, instance.choose(context, client).value)
        else -> throw IllegalStateException(
          "Task '${decider.id}' is a switch, but '${instance.javaClass.name}' is not a SwitchTask",
        )
      }
    return Decision(chosen.id, options.filterNot { it === chosen }.map { it.id })
  }

  private fun byClass(
    decider: TaskDef,
    definition: Class<out Task>?,
  ): TaskDef =
    options.firstOrNull { it.definition == definition }
      ?: throw IllegalArgumentException(
        "Switch '${decider.id}' chose ${definition?.name ?: "nothing"}, which is not one of its cases: ${describeCases()}",
      )

  private fun byId(
    decider: TaskDef,
    taskId: String,
  ): TaskDef =
    options.firstOrNull { it.id == taskId }
      ?: throw IllegalArgumentException(
        "Switch '${decider.id}' chose '$taskId', which is not one of its cases: ${describeCases()}",
      )

  private fun describeCases(): String = options.joinToString { "'${it.id}'" }
}
