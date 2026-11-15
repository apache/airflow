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
 * The text of an edge label. The Airflow UI shows the label on the edge in
 * the graph, as it does for Python's `Label`. The text comes before the task
 * that the label is put on. That way the text sits between the two tasks of
 * the edge in the code, just as the label sits between them in the graph:
 *
 * ```java
 * loaded.before(Label.of("when empty").on(reportEmpty)); // load >> Label("when empty") >> report_empty
 * cleaned.after(Label.of("always").on(loaded));          // load >> Label("always") >> cleanup
 * ```
 */
class Label private constructor(
  private val text: String,
) {
  /**
   * Labels each edge that a call to [Deps.Flow.before] or [Deps.Flow.after]
   * draws between the call's receiver and [first] or one of [rest].
   *
   * Each edge of a fan-out can therefore carry its own label, and one label
   * can go on several edges:
   *
   * ```java
   * checked.before(Label.of("rows found").on(processed), Label.of("no rows").on(reportEmpty));
   * // load >> Label("forks") >> [report_empty, report_suspicious]
   * loaded.before(Label.of("forks").on(reportEmpty, reportSuspicious));
   * ```
   *
   * Passing a task's handle into another task's arguments declares an
   * edge. Calling `Then`, `Else` or `Case` also declares an edge. To label
   * one of those edges, declare it again with a label. Declaring an edge
   * again only applies the label. A new label replaces the label the edge
   * had, as Python's `DAG.set_edge_info` does. Declaring the edge again
   * without a label keeps its label.
   *
   * When an edge goes to or from a task group, the UI draws it to or from
   * the group's own node and shows the label there. A label never changes
   * which tasks an edge connects. Python's `Label` can change those tasks.
   * It does so when the end written to the left of the `Label` is in a task
   * group and the other end is not in that same group. Python then replaces
   * the left end with the group that contains it. For example, if `v` is in
   * group `g` and `n` is in no group, `v >> Label("x") >> n` draws edges
   * from the last tasks in `g` instead of from `v`. In Java, the end that
   * matches this left end is the receiver of [Deps.Flow.before] or
   * [Deps.Flow.after].
   *
   * @param first The task, task group, or set of tasks and groups at one end
   *    of each edge that gets the label.
   * @param rest More tasks, task groups, or sets, labeled the same way as
   *    [first].
   * @return A point in the flow to pass to [Deps.Flow.before] or
   *    [Deps.Flow.after]. Calling [Deps.Flow.before] or [Deps.Flow.after] on
   *    this point fails, because the label goes on the edges between the
   *    receiver of that call and the flows passed to this method.
   */
  fun on(
    first: Deps.Flow,
    vararg rest: Deps.Flow,
  ): Deps.Flow = LabeledFlow(FlowSet(listOf(first, *rest)), text)

  companion object {
    /**
     * Makes a label with [text], to put on edges with [on].
     *
     * @throws IllegalArgumentException if [text] is blank.
     */
    @JvmStatic
    fun of(text: String): Label {
      require(text.isNotBlank()) { "An edge label cannot be blank; pass the text to show on the edge" }
      return Label(text)
    }
  }
}
