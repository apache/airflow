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
 * A task whose boolean decides which of two tasks runs; the other is skipped.
 *
 * Register one with [DagDef.If], then name the sides with [ConditionRef.Then]
 * and [ConditionRef.Else]:
 *
 * ```java
 * public class HasRows implements ConditionTask {
 *   @Override
 *   public boolean decide(Context context, Client client) {
 *     return ((Number) client.getXCom("extract")).longValue() > 0;
 *   }
 * }
 * ```
 *
 * The SDK runs [decide] and pushes its result as this task's return value, so
 * [execute] is never called.
 *
 * @see DagDef.If
 */
interface ConditionTask : Task {
  /**
   * Decides which side of the condition runs.
   *
   * Any exception thrown marks the task instance as failed, and nothing is
   * then pushed or skipped.
   *
   * @param context Runtime context for the current execution workload.
   * @param client Client for Airflow API calls scoped to this execution.
   * @return True to run the [ConditionRef.Then] side, false the
   *    [ConditionRef.Else] side.
   * @throws Exception on failure; the task instance is marked failed.
   */
  @Throws(Exception::class)
  fun decide(
    context: Context,
    client: Client,
  ): Boolean

  /** Never called: the SDK runs a condition through [decide]. */
  override fun execute(
    context: Context,
    client: Client,
  ): Unit =
    throw IllegalStateException(
      "Condition '${javaClass.name}' runs through decide(), so execute() is never called",
    )
}

/**
 * A condition registered with a Dag: name the task each outcome runs.
 *
 * ```java
 * dag.If(HasRows.class).Then(load).Else(reportEmpty);
 * ```
 *
 * A named task runs after the condition, so naming it records that edge, as
 * [Deps.Flow.before] would. The condition skips only the side not taken; what
 * runs after both sides needs a trigger rule that tolerates one skipped
 * upstream, such as `none_failed_min_one_success`.
 *
 * @see DagDef.If
 */
class ConditionRef private constructor(
  private val ref: TaskRef<Boolean>,
  private val sides: ConditionDef,
) : Deps.Flow {
  companion object {
    /**
     * @suppress
     *
     * Marks a registered task as a condition. Public so a generated wiring
     * view can call it; user code reaches a condition through [DagDef.If].
     *
     * A wiring view gets the same task back each time it names it, so a second
     * call reuses the condition rather than declaring it again. [DagDef.If] and
     * [TaskGroupRef.If] never reach that path, since registering a task rejects
     * a duplicate ID first.
     */
    @JvmStatic
    fun of(ref: TaskRef<Boolean>): ConditionRef {
      ref.def.decider?.let { existing ->
        require(existing is ConditionDef) { "Task '${ref.def.id}' already decides what to skip as a switch" }
        return ConditionRef(ref, existing)
      }
      return ConditionRef(ref, ConditionDef().also { ref.def.decider = it })
    }
  }

  /** Airflow task ID of the deciding task. */
  val id: String get() = ref.def.id

  /**
   * Names the task that runs when the condition holds.
   *
   * @param task Task of the same Dag, which the condition skips when it does
   *    not hold.
   * @return This condition, so [Else] can follow.
   * @throws IllegalArgumentException if the task belongs to another Dag, or
   *    this side is already named.
   */
  @Suppress("ktlint:standard:function-naming")
  fun Then(task: TaskRef<*>): ConditionRef = name("Then", task) { sides.whenTrue = it }

  /**
   * Names the task that runs when the condition does not hold. A condition
   * needs no `Else`; without one, nothing is skipped when it holds.
   *
   * @param task Task of the same Dag, which the condition skips when it holds.
   * @return This condition, for chaining.
   * @throws IllegalArgumentException if the task belongs to another Dag, is
   *    the `Then` task, or this side is already named.
   */
  @Suppress("ktlint:standard:function-naming")
  fun Else(task: TaskRef<*>): ConditionRef = name("Else", task) { sides.whenFalse = it }

  /**
   * Sets one task-level configuration value on the deciding task.
   *
   * @param key Airflow task setting name.
   * @param value Value matching the key's schema type.
   * @return This condition, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): ConditionRef {
    ref.config(key, value)
    return this
  }

  override fun nodes(): List<TaskDef> = ref.nodes()

  override fun before(vararg next: Deps.Flow): ConditionRef {
    ref.before(*next)
    return this
  }

  override fun after(vararg previous: Deps.Flow): ConditionRef {
    ref.after(*previous)
    return this
  }

  private fun name(
    side: String,
    task: TaskRef<*>,
    assign: (TaskDef) -> Unit,
  ): ConditionRef {
    sides.check(ref.def, side, task.def)
    assign(task.def)
    // The side runs after the condition, which is what puts the condition in
    // the serialized Dag as its upstream and lets registration see a cycle
    // that runs through a condition.
    ref.before(task)
    return this
  }
}

/** The two sides of one condition, as [ConditionRef] named them. */
internal class ConditionDef : DeciderDef {
  var whenTrue: TaskDef? = null
  var whenFalse: TaskDef? = null

  override val cases: List<TaskDef> get() = listOfNotNull(whenTrue, whenFalse)

  override fun describe(decider: TaskDef): String? =
    if (whenTrue == null) {
      "Condition '${decider.id}' names no task to run when it holds; call Then(...)"
    } else {
      null
    }

  override fun decide(
    decider: TaskDef,
    instance: Task,
    context: Context,
    client: Client,
  ): Decision {
    val held = (instance as ConditionTask).decide(context, client)
    return Decision(held, listOfNotNull((if (held) whenFalse else whenTrue)?.id))
  }

  fun check(
    decider: TaskDef,
    side: String,
    task: TaskDef,
  ) {
    requireUnregistered(decider, "Condition")
    val named = if (side == "Then") whenTrue else whenFalse
    require(named == null) {
      "Condition '${decider.id}' already runs '${named!!.id}' on its $side side; name each side once"
    }
    requireSameDag(decider, task, "Condition")
    require(task !== whenTrue && task !== whenFalse) {
      "Condition '${decider.id}' already runs '${task.id}' on its other side, so the condition " +
        "would decide nothing"
    }
  }
}
