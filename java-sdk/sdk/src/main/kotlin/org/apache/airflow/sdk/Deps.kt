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

import org.apache.airflow.sdk.internal.Refs

/**
 * Vocabulary for declaring a Dag's task graph in Java, and the base of every
 * generated `<Dag>Deps` wiring view.
 *
 * A [Builder.Deps] class inherits [lit] and [Flow] by simple name, so it needs
 * no import and `Flow` does not collide with `java.util.concurrent.Flow`.
 */
interface Deps {
  /**
   * A point in the task graph: one task, one task group, or a set of them.
   *
   * [Flow] declares a dependency where nothing flows but the ordering. An
   * edge that carries a value is declared by passing the upstream's handle
   * instead.
   */
  interface Flow {
    /** The tasks at this point in the flow. */
    fun nodes(): List<TaskDef>

    /**
     * The ends an edge drawn here attaches to. A task stands for itself, so
     * the default is [nodes]; a task group stands for the group rather than
     * for the tasks it holds today.
     */
    fun endpoints(): List<Endpoint> = nodes()

    /**
     * Runs the tasks here before each of [next], carrying no value.
     *
     * ```java
     * loaded.before(cleaned, notified); // cleanup and notify both wait for load
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
      next.forEach { link(this, it) }
      return this
    }

    /**
     * Runs the tasks here after each of [previous], carrying no value.
     *
     * ```java
     * cleaned.after(loaded, transformed); // cleanup waits for load and transform
     * ```
     *
     * @param previous Tasks that run before the ones here.
     * @return This point in the flow.
     */
    fun after(vararg previous: Flow): Flow {
      previous.forEach { link(it, this) }
      return this
    }

    companion object {
      /**
       * Treats several tasks as one point in the flow, so a single call draws
       * every edge between two sets:
       *
       * ```java
       * Flow.of(a, b).before(c, d); // c and d both wait for a and b
       * ```
       */
      @JvmStatic
      fun of(vararg flows: Flow): Flow = FlowSet(flows.toList())
    }
  }

  /**
   * One task group of the Dag being wired: a point in the flow, and the
   * namespace of the tasks and groups declared inside it.
   *
   * The generated wiring view nests one of these per [Builder.TaskGroup]
   * class, so a group is reached by calling it and its contents by calling on
   * through:
   *
   * ```java
   * staging().stage(rows);          // the task "staging.stage"
   * staging().checks().nulls(id);   // the task "staging.checks.nulls"
   * extract().before(staging());    // the whole group runs after extract
   * ```
   */
  interface TaskGroup : Flow {
    /** Full ID of this group, as the Dag registered it. */
    fun groupId(): String

    override fun nodes(): List<TaskDef> = Refs.group(groupId()).nodes()

    // The group itself, not its tasks, so an edge drawn before its tasks
    // exist still reaches them.
    override fun endpoints(): List<Endpoint> = listOf(Refs.group(groupId()))
  }

  /**
   * Wraps an inline constant as a task argument, as in
   * `transform(extract(), lit(0.9))`. It is passed to the task as a constant
   * and creates no dependency edge.
   *
   * @param value Constant to bind; may be null for a nullable parameter.
   */
  fun <T> lit(value: T?): Arg<T> = Arg.lit(value)
}

/** Several tasks or groups as one point in the flow, which no single handle can represent. */
internal class FlowSet(
  internal val flows: List<Deps.Flow>,
) : Deps.Flow {
  override fun nodes(): List<TaskDef> = flows.flatMap { it.nodes() }

  override fun endpoints(): List<Endpoint> = flows.flatMap { it.endpoints() }
}

/**
 * Draws an ordering edge from each endpoint of [upstream] to each of
 * [downstream]. An edge between two tasks is recorded on the downstream task;
 * one with a task group at either end is recorded on the group's Dag, and
 * means whatever tasks the group holds when the Dag is registered.
 */
private fun link(
  upstream: Deps.Flow,
  downstream: Deps.Flow,
) {
  for (up in upstream.endpoints()) {
    for (down in downstream.endpoints()) {
      if (up is TaskDef && down is TaskDef) {
        down.dependsOn(up)
      } else {
        val upDag = up.owningDag
        val downDag = down.owningDag
        require(upDag == null || downDag == null || upDag === downDag) {
          "Cannot order ${up.label} of Dag '${upDag?.id}' before ${down.label} of " +
            "Dag '${downDag?.id}'; an edge stays inside one Dag"
        }
        (upDag ?: downDag)?.let { it.groupEdges += up to down }
      }
    }
  }
}

/** The Dag an endpoint belongs to, null for a task not registered with one yet. */
internal val Endpoint.owningDag: DagDef?
  get() =
    when (this) {
      is TaskDef -> owner
      is TaskGroupRef -> dag
    }

/** How an endpoint is named in a diagnostic. */
internal val Endpoint.label: String
  get() =
    when (this) {
      is TaskDef -> "task '$id'"
      is TaskGroupRef -> "task group '$id'"
    }
