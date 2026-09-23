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
   * A point in the task graph: one task, or the set of tasks a [then] just
   * pointed at.
   *
   * [Flow] is Java's spelling of Python's `>>`, for a dependency where
   * nothing flows but the ordering.
   */
  interface Flow {
    /** The tasks at this point in the flow. */
    fun nodes(): List<TaskDef>

    /**
     * Wires an ordering-only edge from every task here to each of [next],
     * and returns the new frontier — the set just pointed at — so a chain
     * walks through a fan:
     *
     * ```java
     * a.then(b);            // a >> b
     * a.then(b, c).then(d); // a >> [b, c] >> d
     * ```
     *
     * @param next Tasks that run after the ones here.
     * @return The tasks just pointed at, to continue the chain from.
     */
    fun then(vararg next: Flow): Flow {
      val upstreams = nodes()
      val frontier = next.flatMap { it.nodes() }
      frontier.forEach { downstream -> upstreams.forEach { downstream.dependsOn(it) } }
      return FlowSet(frontier)
    }

    companion object {
      /**
       * Opens a chain from a set of tasks, which [then] cannot do on its own:
       * Java has no list literal to call `.then` on.
       *
       * ```java
       * Deps.Flow.of(a, b).then(c); // [a, b] >> c
       * ```
       */
      @JvmStatic
      fun of(vararg flows: Flow): Flow = FlowSet(flows.flatMap { it.nodes() })
    }
  }
}

/** A frontier of more than one task, which no single [TaskRef] can represent. */
internal class FlowSet(
  private val nodes: List<TaskDef>,
) : Deps.Flow {
  override fun nodes(): List<TaskDef> = nodes
}
