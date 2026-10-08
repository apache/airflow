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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class TaskGroupTest {
  private fun upstreamIds(
    dag: DagDef,
    taskId: String,
  ) = dag.expandGroupEdges().upstreamsOf(dag.tasks.getValue(taskId))

  @Test
  @DisplayName("Should prefix the IDs of tasks and groups declared in a group")
  fun shouldPrefixIdsDeclaredInGroup() {
    val dag = DagDef("d")
    val staging = dag.taskGroup("staging")
    val stage = staging.task<Unit>("stage", NoopTask::class.java)
    val checks = staging.taskGroup("checks")
    checks.task<Unit>("nulls", NoopTask::class.java)

    assertEquals("staging.stage", stage.def.id)
    assertEquals("staging.checks", checks.id)
    assertEquals(listOf("staging.stage", "staging.checks.nulls"), dag.tasks.keys.toList())
    assertEquals(listOf("staging", "staging.checks"), dag.groups.keys.toList())
  }

  @Test
  @DisplayName("Should reject a group ID that is not a plain identifier")
  fun shouldRejectInvalidGroupId() {
    val error = assertThrows(IllegalArgumentException::class.java) { DagDef("d").taskGroup("a.b") }

    assertEquals(
      "Task group ID 'a.b' must contain only ASCII letters, digits, underscores, or dashes",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a group ID that a task already uses")
  fun shouldRejectGroupIdTakenByTask() {
    val dag = DagDef("d")
    dag.task<Unit>("staging", NoopTask::class.java)

    val error = assertThrows(IllegalArgumentException::class.java) { dag.taskGroup("staging") }

    assertEquals("Dag 'd' already has a task or task group with ID: staging", error.message)
  }

  @Test
  @DisplayName("Should reject a task ID that a group already uses")
  fun shouldRejectTaskIdTakenByGroup() {
    val dag = DagDef("d")
    dag.taskGroup("staging")

    val error =
      assertThrows(IllegalArgumentException::class.java) { dag.task<Unit>("staging", NoopTask::class.java) }

    assertEquals("Dag 'd' already has a task group with ID: staging", error.message)
  }

  @Test
  @DisplayName("Should wire a group upstream from its leaves and downstream to its roots on registration")
  fun shouldExpandGroupEdgesOntoRootsAndLeaves() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val staging = dag.taskGroup("staging")
    val stage = staging.task<Unit>("stage", NoopTask::class.java)
    staging.taskGroup("checks").task<Unit>("nulls", NoopTask::class.java).after(stage)
    val publish = dag.taskGroup("publish")
    publish.task<Unit>("push", NoopTask::class.java)
    val load = dag.task<Unit>("load", NoopTask::class.java)
    extract.before(staging)
    staging.before(publish)
    publish.before(load)

    Bundle().register(dag)

    assertEquals(setOf("extract"), upstreamIds(dag, "staging.stage"))
    assertEquals(setOf("staging.stage"), upstreamIds(dag, "staging.checks.nulls"))
    assertEquals(setOf("staging.checks.nulls"), upstreamIds(dag, "publish.push"))
    assertEquals(setOf("publish.push"), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should record each group's own edges the way Python's TaskGroup does")
  fun shouldRecordGroupEdgesOnGroups() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val staging = dag.taskGroup("staging")
    staging.task<Unit>("stage", NoopTask::class.java)
    val publish = dag.taskGroup("publish")
    publish.task<Unit>("push", NoopTask::class.java)
    val load = dag.task<Unit>("load", NoopTask::class.java)
    extract.before(staging)
    staging.before(publish)
    publish.before(load)

    Bundle().register(dag)

    val edges = dag.expandGroupEdges()
    assertEquals(setOf("extract"), edges.edgesOf(staging.id).upstreamTaskIds)
    assertEquals(setOf("publish"), edges.edgesOf(staging.id).downstreamGroupIds)
    assertEquals(emptySet<String>(), edges.edgesOf(staging.id).downstreamTaskIds)
    assertEquals(setOf("staging"), edges.edgesOf(publish.id).upstreamGroupIds)
    assertEquals(setOf("staging.stage"), edges.edgesOf(publish.id).upstreamTaskIds)
    assertEquals(setOf("load"), edges.edgesOf(publish.id).downstreamTaskIds)
  }

  @Test
  @DisplayName("Should step over a group with no tasks to the tasks beyond it")
  fun shouldStepOverEmptyGroup() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val empty = dag.taskGroup("empty")
    val load = dag.task<Unit>("load", NoopTask::class.java)
    extract.before(empty)
    empty.before(load)

    Bundle().register(dag)

    assertEquals(setOf("extract"), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should step over an empty nested group to the tasks of the group holding it")
  fun shouldStepOverEmptyNestedGroup() {
    val dag = DagDef("d")
    val outer = dag.taskGroup("outer")
    outer.task<Unit>("t", NoopTask::class.java)
    val inner = outer.taskGroup("inner")
    val load = dag.task<Unit>("load", NoopTask::class.java)
    inner.before(load)

    Bundle().register(dag)

    assertEquals(setOf("outer.t"), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should prefer what already runs before an empty group over the group holding it")
  fun shouldPreferEarlierEdgeOverParentForEmptyGroup() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val outer = dag.taskGroup("outer")
    outer.task<Unit>("t", NoopTask::class.java)
    val inner = outer.taskGroup("inner")
    val load = dag.task<Unit>("load", NoopTask::class.java)
    extract.before(inner)
    inner.before(load)

    Bundle().register(dag)

    assertEquals(setOf("extract"), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should resolve a group's endpoints in the order the edges were drawn")
  fun shouldResolveGroupEndpointsInDrawingOrder() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val outer = dag.taskGroup("outer")
    val t = outer.task<Unit>("t", NoopTask::class.java)
    val inner = outer.taskGroup("inner")
    inner.task<Unit>("i", NoopTask::class.java)
    extract.before(outer)
    inner.before(t)

    Bundle().register(dag)

    // outer had both tasks as roots when the first edge was drawn, so extract reaches both.
    assertEquals(setOf("extract", "outer.inner.i"), upstreamIds(dag, "outer.t"))
    assertEquals(setOf("extract"), upstreamIds(dag, "outer.inner.i"))
  }

  @Test
  @DisplayName("Should resolve a group's endpoints against the edges drawn before it")
  fun shouldResolveGroupEndpointsAgainstEarlierEdges() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val outer = dag.taskGroup("outer")
    val t = outer.task<Unit>("t", NoopTask::class.java)
    val inner = outer.taskGroup("inner")
    inner.task<Unit>("i", NoopTask::class.java)
    inner.before(t)
    extract.before(outer)

    Bundle().register(dag)

    // outer's only root once inner runs before t, so extract reaches nothing else.
    assertEquals(setOf("outer.inner.i"), upstreamIds(dag, "outer.t"))
    assertEquals(setOf("extract"), upstreamIds(dag, "outer.inner.i"))
  }

  @Test
  @DisplayName("Should reject a group edge that crosses two Dags")
  fun shouldRejectCrossDagGroupEdge() {
    val staging = DagDef("a").taskGroup("staging")
    val publish = DagDef("b").taskGroup("publish")

    val error = assertThrows(IllegalArgumentException::class.java) { staging.before(publish) }

    assertEquals(
      "Cannot order task group 'staging' of Dag 'a' before task group 'publish' of Dag 'b'; " +
        "an edge stays inside one Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should wire a task added to a group after the Dag was registered")
  fun shouldWireTaskAddedAfterRegistration() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val staging = dag.taskGroup("staging")
    staging.task<Unit>("stage", NoopTask::class.java)
    extract.before(staging)
    Bundle().register(dag)

    staging.task<Unit>("late", NoopTask::class.java)

    assertEquals(setOf("extract"), upstreamIds(dag, "staging.late"))
  }

  @Test
  @DisplayName("Should expand an edge drawn before the group's tasks were declared")
  fun shouldExpandEdgeDrawnBeforeGroupFilled() {
    val dag = DagDef("d")
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val staging = dag.taskGroup("staging")
    extract.before(staging)
    staging.task<Unit>("stage", NoopTask::class.java)

    Bundle().register(dag)

    assertEquals(setOf("extract"), upstreamIds(dag, "staging.stage"))
  }

  @Test
  @DisplayName("Should draw edges for every group and task in a combined flow")
  fun shouldWireGroupsInCombinedFlow() {
    val dag = DagDef("d")
    val staging = dag.taskGroup("staging")
    staging.task<Unit>("stage", NoopTask::class.java)
    val extract = dag.task<Unit>("extract", NoopTask::class.java)
    val load = dag.task<Unit>("load", NoopTask::class.java)
    Deps.Flow.of(staging, extract).before(load)

    Bundle().register(dag)

    assertEquals(setOf("staging.stage", "extract"), upstreamIds(dag, "load"))
  }

  @Test
  @DisplayName("Should reject a cycle that a group edge closes")
  fun shouldRejectCycleThroughGroup() {
    val dag = DagDef("d")
    val staging = dag.taskGroup("staging")
    val stage = staging.task<Unit>("stage", NoopTask::class.java)
    stage.before(staging)

    val error = assertThrows(IllegalArgumentException::class.java) { Bundle().register(dag) }

    assertEquals("Task dependencies in Dag 'd' contain a cycle involving task 'staging.stage'", error.message)
  }
}
