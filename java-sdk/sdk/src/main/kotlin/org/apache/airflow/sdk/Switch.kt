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
 * A task that chooses one of several tasks to run; every other one is skipped.
 *
 * Register one with [DagDef.Switch], then list what it can choose with
 * [SwitchRef.Case]. The choice is the class of the task to run:
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
 * Only a [Task] class compiles as a choice. javac cannot tell whether it is a
 * case of this switch, so a Task class that is not one compiles and fails when
 * the task runs: the task instance is marked failed, and nothing is pushed or
 * skipped.
 *
 * Because a switch names a case by its class, two tasks that run the same
 * class cannot both be cases of it; the second [SwitchRef.Case] call throws.
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
     *
     * A wiring view gets the same task back each time it names it, so a second
     * call reuses the switch rather than declaring it again. [DagDef.Switch] and
     * [TaskGroupRef.Switch] never reach that path, since registering a task
     * rejects a duplicate ID first.
     */
    @JvmStatic
    fun of(ref: TaskRef<*>): SwitchRef {
      ref.def.decider?.let { existing ->
        require(existing is SwitchDef) { "Task '${ref.def.id}' already decides what to skip as a condition" }
        return SwitchRef(ref, existing)
      }
      val definition = ref.def.definition
      require(SwitchTask::class.java.isAssignableFrom(definition)) {
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
   * @throws IllegalArgumentException if the task belongs to another Dag, is
   *    already a case of this switch, or runs the same class as one.
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
    // A switch names its case by class, so two cases sharing one would be
    // indistinguishable.
    options.firstOrNull { it.definition == case.definition }?.let { first ->
      throw IllegalArgumentException(
        "Switch '${decider.id}' cannot choose between '${first.id}' and '${case.id}': both run " +
          "'${case.definition.name}', and a switch names its case by class",
      )
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

  private fun describeCases(): String = options.joinToString { "'${it.id}'" }
}
