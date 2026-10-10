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

import org.apache.airflow.sdk.Deps.Flow
import org.apache.airflow.sdk.execution.serializeDag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class EdgeLabelTest {
  private fun edgeInfo(dag: DagDef): Any? = serializeDag(dag, "", ".")["edge_info"]

  private fun labels(vararg edges: Triple<String, String, String>): Map<String, Map<String, Map<String, String>>> =
    edges
      .groupBy { it.first }
      .mapValues { (_, fromOne) -> fromOne.associate { (_, downstream, label) -> downstream to mapOf("label" to label) } }

  private fun upstreamIds(
    dag: DagDef,
    taskId: String,
  ) = dag.expandGroupEdges().upstreamsOf(dag.tasks.getValue(taskId))

  private fun DagDef.noop(id: String) = task<Unit>(id, NoopTask::class.java)

  private fun TaskGroupRef.noop(id: String) = task<Unit>(id, NoopTask::class.java)

  @Test
  @DisplayName("Should label the edge that before draws to the flow it wraps")
  fun shouldLabelEdgeDrawnByBefore() {
    val dag = DagDef("d")
    val load = dag.noop("load")
    val reportEmpty = dag.noop("report_empty")

    load.before(Flow.label(reportEmpty, "when empty"))

    assertEquals(setOf("load"), upstreamIds(dag, "report_empty"))
    assertEquals(labels(Triple("load", "report_empty", "when empty")), edgeInfo(dag))
  }

  @Test
  @DisplayName("Should label the edge from the flow that after wraps")
  fun shouldLabelEdgeDrawnByAfter() {
    val dag = DagDef("d")
    val load = dag.noop("load")
    val cleanup = dag.noop("cleanup")

    cleanup.after(Flow.label(load, "always"))

    assertEquals(setOf("load"), upstreamIds(dag, "cleanup"))
    assertEquals(labels(Triple("load", "cleanup", "always")), edgeInfo(dag))
  }

  @Test
  @DisplayName("Should give each edge of a fan-out its own label, and none to an edge without one")
  fun shouldLabelEachEdgeOfFanOut() {
    val dag = DagDef("d")
    val check = dag.noop("check")
    val process = dag.noop("process")
    val reportEmpty = dag.noop("report_empty")
    val audit = dag.noop("audit")

    check.before(Flow.label(process, "rows found"), Flow.label(reportEmpty, "no rows"), audit)

    assertEquals(setOf("check"), upstreamIds(dag, "audit"))
    assertEquals(
      labels(Triple("check", "process", "rows found"), Triple("check", "report_empty", "no rows")),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should label every edge to a set of flows, with the outer label of a labeled flow winning")
  fun shouldLabelEveryEdgeToASet() {
    val dag = DagDef("d")
    val left = dag.noop("left")
    val right = dag.noop("right")
    val staging = dag.taskGroup("staging")
    staging.noop("stage")
    val join = dag.noop("join")

    join.after(Flow.label(Flow.of(Flow.label(left, "inner"), Flow.label(staging, "inner"), right), "done"))

    assertEquals(
      labels(
        Triple("left", "join", "done"),
        Triple("right", "join", "done"),
        Triple("staging.downstream_join_id", "join", "done"),
      ),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should only add the label to an edge that a condition already declared")
  fun shouldLabelEdgeDeclaredAlready() {
    val dag = DagDef("d")
    val load = dag.noop("load")
    val reportEmpty = dag.noop("report_empty")
    val gate = dag.If(HasRows::class.java).Then(load).Else(reportEmpty)

    gate.before(Flow.label(load, "rows found"), Flow.label(reportEmpty, "no rows"))

    assertEquals(listOf(dag.tasks.getValue("hasRows")), load.def.upstreams.toList())
    assertEquals(
      labels(Triple("hasRows", "load", "rows found"), Triple("hasRows", "report_empty", "no rows")),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should replace an earlier label, and keep it when the edge is declared again without one")
  fun shouldReplaceEarlierLabel() {
    val dag = DagDef("d")
    val load = dag.noop("load")
    val cleanup = dag.noop("cleanup")
    val publish = dag.taskGroup("publish")
    publish.noop("push")

    load.before(Flow.label(cleanup, "always"), Flow.label(publish, "always"))
    load.before(Flow.label(cleanup, "when empty"), Flow.label(publish, "when empty"))
    load.before(cleanup, publish)
    cleanup.after(load)

    assertEquals(
      labels(
        Triple("load", "cleanup", "when empty"),
        Triple("load", "publish.push", "when empty"),
        Triple("load", "publish.upstream_join_id", "when empty"),
      ),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should reject before or after called on a labeled flow, and draw no edge")
  fun shouldRejectLabeledReceiver() {
    val dag = DagDef("d")
    val load = dag.noop("load")
    val cleanup = dag.noop("cleanup")
    val audit = dag.noop("audit")

    val calls =
      mapOf<String, () -> Unit>(
        "before" to { Flow.label(load, "always").before(cleanup) },
        "after" to { Flow.label(load, "always").after(cleanup) },
        "before on a set" to { Flow.of(audit, Flow.label(load, "always")).before(cleanup) },
      )

    calls.forEach { (call, draw) ->
      val error = assertThrows(IllegalArgumentException::class.java, { draw() }, call)
      assertEquals(
        "Cannot call ${call.substringBefore(' ')} on task 'load' labeled \"always\": a label goes on the " +
          "edges between the receiver of before or after and the flow it wraps, so pass Flow.label(...) to " +
          "before or after as an argument instead",
        error.message,
        call,
      )
    }
    assertEquals(emptySet<String>(), upstreamIds(dag, "cleanup"))
    assertEquals(emptySet<String>(), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should reject an empty or blank label")
  fun shouldRejectBlankLabel() {
    val load = DagDef("d").noop("load")

    listOf("", "  ").forEach { text ->
      val error = assertThrows(IllegalArgumentException::class.java, { Flow.label(load, text) }, "\"$text\"")
      assertEquals("An edge label cannot be blank; pass the text to show on the edge", error.message)
    }
  }

  @Test
  @DisplayName("Should put the label of an edge to or from a group under the group's join node")
  fun shouldLabelGroupEdgesAtJoinNodes() {
    val dag = DagDef("d")
    val extract = dag.noop("extract")
    val transform = dag.taskGroup("transform")
    transform.noop("clean")
    val publish = dag.taskGroup("publish")
    publish.noop("push")
    val notify = dag.noop("notify")

    extract.before(Flow.label(transform, "to transform"))
    transform.before(Flow.label(publish, "to publish"))
    publish.before(Flow.label(notify, "to notify"))

    // Python also labels the edge from extract to the group's first task, when no group holds extract.
    assertEquals(
      labels(
        Triple("extract", "transform.clean", "to transform"),
        Triple("extract", "transform.upstream_join_id", "to transform"),
        Triple("transform.downstream_join_id", "publish.upstream_join_id", "to publish"),
        Triple("publish.downstream_join_id", "notify", "to notify"),
      ),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should label the task edges of a group edge from a task in no group, whichever verb draws it")
  fun shouldLabelTaskEdgesOfGroupEdgeEitherWay() {
    val dag = DagDef("d")
    val extract = dag.noop("extract")
    val transform = dag.taskGroup("transform")
    transform.noop("clean")

    transform.after(Flow.label(extract, "rows"))

    assertEquals(
      labels(Triple("extract", "transform.clean", "rows"), Triple("extract", "transform.upstream_join_id", "rows")),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should leave the task edges of a group edge from a task in a group unlabeled")
  fun shouldNotLabelTaskEdgesOfGroupEdgeFromGroupedTask() {
    val dag = DagDef("d")
    val transform = dag.taskGroup("transform")
    val clean = transform.noop("clean")
    val nulls = transform.taskGroup("checks").noop("nulls")
    val publish = dag.taskGroup("publish")
    publish.noop("push")

    Flow.of(clean, nulls).before(Flow.label(publish, "rows"))

    assertEquals(setOf("transform.clean", "transform.checks.nulls"), upstreamIds(dag, "publish.push"))
    assertEquals(
      labels(
        Triple("transform.clean", "publish.upstream_join_id", "rows"),
        Triple("transform.checks.nulls", "publish.upstream_join_id", "rows"),
      ),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should keep the label a task edge has of its own over the one its group edge gives it")
  fun shouldKeepTaskEdgeLabelOverGroupEdgeLabel() {
    val dag = DagDef("d")
    val extract = dag.noop("extract")
    val transform = dag.taskGroup("transform")
    val clean = transform.noop("clean")
    val validate = transform.noop("validate")
    extract.before(Flow.label(clean, "rows"), validate)

    extract.before(Flow.label(transform, "to transform"))

    // validate's edge has no label of its own, so it takes the label of the group edge.
    // Python gives the edge the same label.
    assertEquals(
      labels(
        Triple("extract", "transform.clean", "rows"),
        Triple("extract", "transform.validate", "to transform"),
        Triple("extract", "transform.upstream_join_id", "to transform"),
      ),
      edgeInfo(dag),
    )
  }

  @Test
  @DisplayName("Should let a later group edge relabel a task edge that an earlier one labeled")
  fun shouldLetLaterGroupEdgeRelabelTaskEdge() {
    val dag = DagDef("d")
    val extract = dag.noop("extract")
    val outer = dag.taskGroup("outer")
    val inner = outer.taskGroup("inner")
    inner.noop("i")

    extract.before(Flow.label(outer, "a"))
    extract.before(Flow.label(inner, "b"))

    assertEquals(
      labels(
        Triple("extract", "outer.inner.i", "b"),
        Triple("extract", "outer.upstream_join_id", "a"),
        Triple("extract", "outer.inner.upstream_join_id", "b"),
      ),
      edgeInfo(dag),
    )
  }
}
