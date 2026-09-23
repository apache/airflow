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

/**
 * A value a task can be given: the output of an upstream task, carried by the
 * [TaskRef] that task's registration returned, or an inline constant.
 *
 * A constant has to be wrapped because a bare `Double` cannot implement this
 * type: boxed types only, no primitives.
 *
 * @param T Type of the value.
 */
sealed class Arg<T> {
  companion object {
    /**
     * Wraps an inline constant as a task argument, passed to the task as a
     * constant and creating no dependency edge.
     *
     * [Deps.lit] is the spelling a [Builder.Deps] class uses; this is the same
     * call for code with no `Deps` in scope.
     *
     * @param value Constant to bind; may be null for a nullable parameter.
     */
    @JvmStatic
    fun <T> lit(value: T?): Arg<T> = LiteralArg(value)
  }
}

internal class LiteralArg<T>(
  internal val value: T?,
) : Arg<T>()

/**
 * The output of a registered task, and the task's place in the flow.
 *
 * Passing this handle into another task's arguments feeds this task's return
 * value into that parameter and wires the data edge. [Deps.Flow.before] and
 * [Deps.Flow.after] wire an ordering-only edge instead, where nothing flows
 * but the sequence.
 *
 * @param T Return type of the task this handle refers to.
 */
class TaskRef<T> internal constructor(
  internal val def: TaskDef,
) : Arg<T>(),
  Deps.Flow {
  override fun nodes(): List<TaskDef> = listOf(def)

  override fun before(vararg next: Deps.Flow): TaskRef<T> {
    super<Deps.Flow>.before(*next)
    return this
  }

  override fun after(vararg previous: Deps.Flow): TaskRef<T> {
    super<Deps.Flow>.after(*previous)
    return this
  }

  /**
   * Sets one task-level configuration value, so a task built through
   * [DagDef.task] is configured where it is created.
   *
   * @param key Dag serialization schema property name.
   * @param value Value matching the key's schema type.
   * @return This handle, for chaining.
   * @throws IllegalArgumentException if the key is unknown or the value type
   *    does not match.
   */
  fun config(
    key: String,
    value: Any?,
  ): TaskRef<T> {
    def.config(key, value)
    return this
  }
}
