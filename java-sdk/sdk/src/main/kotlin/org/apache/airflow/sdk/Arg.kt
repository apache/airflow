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
 * A value a task can be given, of which [TaskRef] — the output of an upstream
 * task — is the only form so far.
 *
 * @param T Type of the value.
 */
sealed class Arg<T>

/**
 * The output of a registered task, and the task's place in the flow.
 *
 * [Deps.Flow.before] and [Deps.Flow.after] wire an ordering-only edge from
 * this task, where nothing flows but the sequence.
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
}
