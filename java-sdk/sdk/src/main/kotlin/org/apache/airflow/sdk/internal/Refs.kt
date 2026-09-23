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

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskRef

/**
 * @suppress
 *
 * The recorder behind a `@Builder.Deps` wiring class. Public so that
 * processor-generated wiring views can call it; not user-facing API.
 *
 * A wiring view's methods are `default` methods on an interface, so they hold
 * no Dag of their own. [record] puts the Dag being built in scope for exactly
 * the duration of one `depends()` call, and the view's methods register into
 * it. Nothing parses a syntax tree: identity travels with the [TaskRef] a
 * call returns, so a result held in a local and reused just works.
 */
object Refs {
  private class Recording(
    val dag: DagDef,
  ) {
    val byTaskId = linkedMapOf<String, TaskRef<*>>()
  }

  private val recording = ThreadLocal<Recording?>()

  /**
   * Runs one `depends()` call with [dag] in scope, then returns the Dag the
   * wiring built.
   *
   * @throws IllegalArgumentException if the wiring left a declared task
   *    unregistered.
   */
  @JvmStatic
  fun record(
    dag: DagDef,
    taskIds: List<String>,
    depends: Runnable,
  ): DagDef {
    check(recording.get() == null) { "Dag wiring is already being recorded on this thread" }
    recording.set(Recording(dag))
    try {
      depends.run()
    } finally {
      recording.remove()
    }
    val missing = taskIds.filterNot { it in dag.tasks }
    require(missing.isEmpty()) {
      "Wiring for Dag '${dag.id}' did not register task(s) ${missing.joinToString { "'$it'" }}: " +
        "every @Builder.Task method must be called in the @Builder.Deps class"
    }
    return dag
  }

  /**
   * Records a task that takes no data arguments.
   *
   * @return The handle representing this task, memoized by [TaskDef.id] so every call
   *    yields the same one.
   */
  @JvmStatic
  fun <T> node(def: TaskDef): TaskRef<T> = call(def)

  /**
   * Records a task and the data edge for every [TaskRef] among [args]; a
   * literal argument records a baked value and no edge.
   *
   * @return The handle representing this task, memoized by [TaskDef.id] so a result
   *    held in a local and reused refers to one node.
   */
  @JvmStatic
  @Suppress("UNCHECKED_CAST", "SpreadOperator")
  fun <T> call(
    def: TaskDef,
    vararg args: Arg<*>,
  ): TaskRef<T> {
    val active =
      checkNotNull(recording.get()) {
        "Task '${def.id}' was wired outside a @Builder.Deps class; the wiring view's methods " +
          "only record while the generated builder is running depends()"
      }
    active.byTaskId[def.id]?.let { existing ->
      require(args.isEmpty()) {
        "Task '${def.id}' is wired more than once with arguments; call it once and reuse the handle it returned"
      }
      return existing as TaskRef<T>
    }
    args.filterIsInstance<TaskRef<*>>().forEach { def.dependsOn(it.def) }
    def.inputs += args
    active.dag.addTask(def)
    return TaskRef<T>(def).also { active.byTaskId[def.id] = it }
  }
}
