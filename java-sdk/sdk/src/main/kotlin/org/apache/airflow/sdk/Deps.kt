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

/** Vocabulary for declaring a Dag's task graph in Java. */
interface Deps {
  /**
   * A point in the task graph: one task, or a set of them.
   *
   * [Flow] is Java's spelling of Python's `>>` and `<<`, for a dependency
   * where nothing flows but the ordering. An edge that carries a value is
   * declared by passing the upstream's handle instead.
   */
  interface Flow {
    /** The tasks at this point in the flow. */
    fun nodes(): List<TaskDef>

    /**
     * Runs the tasks here before each of [next], carrying no value — Java's
     * spelling of Python's `>>`.
     *
     * ```java
     * loaded.before(cleaned, notified); // load >> [cleanup, notify]
     * ```
     *
     * Variadic, so one call fans out, and it returns its own receiver: a
     * fan-out has no single next task to hand back. Declaring an edge that
     * already exists changes nothing.
     *
     * @param next Tasks that run after the ones here.
     * @return This point in the flow.
     */
    fun before(vararg next: Flow): Flow {
      val upstreams = nodes()
      next.flatMap { it.nodes() }.forEach { downstream -> upstreams.forEach { downstream.dependsOn(it) } }
      return this
    }

    /**
     * Runs the tasks here after each of [previous], carrying no value —
     * Python's `<<`.
     *
     * ```java
     * cleaned.after(loaded, transformed); // [load, transform] >> cleanup
     * ```
     *
     * @param previous Tasks that run before the ones here.
     * @return This point in the flow.
     */
    fun after(vararg previous: Flow): Flow {
      val downstreams = nodes()
      previous.flatMap { it.nodes() }.forEach { upstream -> downstreams.forEach { it.dependsOn(upstream) } }
      return this
    }

    companion object {
      /**
       * Treats several tasks as one point in the flow, so a single call draws
       * every edge between two sets:
       *
       * ```java
       * Flow.of(a, b).before(c, d); // [a, b] >> [c, d]
       * ```
       */
      @JvmStatic
      fun of(vararg flows: Flow): Flow = FlowSet(flows.flatMap { it.nodes() })
    }
  }
}

/** Several tasks as one point in the flow, which no single [TaskRef] can represent. */
internal class FlowSet(
  private val nodes: List<TaskDef>,
) : Deps.Flow {
  override fun nodes(): List<TaskDef> = nodes
}
