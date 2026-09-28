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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class DagDefTest {
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  @Test
  @DisplayName("Should index tasks by taskId in registration order")
  fun shouldIndexTasksByTaskId() {
    val extract = TaskDef("extract", NoOp::class.java)
    val load = TaskDef("load", NoOp::class.java)

    val dag = DagDef("dag").addTask(extract).addTask(load)

    Assertions.assertEquals(listOf("extract", "load"), dag.tasks.keys.toList())
    Assertions.assertEquals(mapOf("extract" to extract, "load" to load), dag.tasks)
  }

  @Test
  @DisplayName("Should register a task from its id and implementation class")
  fun shouldRegisterTaskFromIdAndImplementationClass() {
    val dag = DagDef("dag").addTask("extract", NoOp::class.java)

    val task = dag.tasks.getValue("extract")
    Assertions.assertEquals("extract", task.id)
    Assertions.assertEquals(NoOp::class.java, task.definition)
    Assertions.assertEquals(dag, task.owner)
  }

  @Test
  @DisplayName("Should reject duplicate task ids")
  fun shouldRejectDuplicateTaskIds() {
    val dag = DagDef("dag").addTask(TaskDef("extract", NoOp::class.java))

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dag.addTask(TaskDef("extract", NoOp::class.java))
      }

    Assertions.assertEquals("Tasks in Dag have duplicate ID: extract", error.message)
  }

  @Test
  @DisplayName("Should reject a task already registered with another dag")
  fun shouldRejectTaskOwnedByAnotherDag() {
    val extract = TaskDef("extract", NoOp::class.java)
    DagDef("first").addTask(extract)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        DagDef("second").addTask(extract)
      }

    Assertions.assertEquals("Task 'extract' already belongs to Dag 'first'", error.message)
  }

  @Test
  @DisplayName("Should reject the same task registered twice with one dag")
  fun shouldRejectTaskRegisteredTwice() {
    val extract = TaskDef("extract", NoOp::class.java)
    val dag = DagDef("dag").addTask(extract)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        dag.addTask(extract)
      }

    Assertions.assertEquals("Task 'extract' already belongs to Dag 'dag'", error.message)
  }

  @Test
  @DisplayName("Should register a task and hand back its handle")
  fun shouldRegisterTaskFromFactory() {
    val dag = DagDef("dag")

    val extract = dag.task<Long>("extract", NoOp::class.java)

    Assertions.assertEquals(listOf("extract"), dag.tasks.keys.toList())
    Assertions.assertEquals(listOf(dag.tasks.getValue("extract")), extract.nodes())
  }

  @Test
  @DisplayName("Should fan out ordering-only edges with before")
  fun shouldFanOutOrderingEdgesWithBefore() {
    val dag = DagDef("dag")
    val extract = dag.task<Long>("extract", NoOp::class.java)
    val left = dag.task<Unit>("left", NoOp::class.java)
    val right = dag.task<Unit>("right", NoOp::class.java)

    extract.before(left, right)

    Assertions.assertEquals(setOf("extract"), upstreamsOf(dag, "left"))
    Assertions.assertEquals(setOf("extract"), upstreamsOf(dag, "right"))
  }

  @Test
  @DisplayName("Should fan in ordering-only edges with after")
  fun shouldFanInOrderingEdgesWithAfter() {
    val dag = DagDef("dag")
    val left = dag.task<Unit>("left", NoOp::class.java)
    val right = dag.task<Unit>("right", NoOp::class.java)
    val join = dag.task<Unit>("join", NoOp::class.java)

    join.after(left, right)

    Assertions.assertEquals(setOf("left", "right"), upstreamsOf(dag, "join"))
  }

  @Test
  @DisplayName("Should return the receiver so before and after chain on one task")
  fun shouldChainBeforeAndAfterOnOneTask() {
    val dag = DagDef("dag")
    val extract = dag.task<Long>("extract", NoOp::class.java)
    val transform = dag.task<Long>("transform", NoOp::class.java)
    val load = dag.task<Unit>("load", NoOp::class.java)

    transform.after(extract).before(load)

    Assertions.assertEquals(setOf("extract"), upstreamsOf(dag, "transform"))
    Assertions.assertEquals(setOf("transform"), upstreamsOf(dag, "load"))
  }

  @Test
  @DisplayName("Should draw every edge between two sets with Flow.of")
  fun shouldDrawEdgesBetweenSetsWithFlowOf() {
    val dag = DagDef("dag")
    val left = dag.task<Unit>("left", NoOp::class.java)
    val right = dag.task<Unit>("right", NoOp::class.java)
    val join = dag.task<Unit>("join", NoOp::class.java)

    Flow.of(left, right).before(join)

    Assertions.assertEquals(setOf("left", "right"), upstreamsOf(dag, "join"))
  }

  private fun upstreamsOf(
    dag: DagDef,
    taskId: String,
  ): Set<String> =
    dag.tasks
      .getValue(taskId)
      .upstreams
      .mapTo(mutableSetOf()) { it.id }
}
