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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class BundleTest {
  @Test
  @DisplayName("Should index dags by dagId")
  fun shouldIndexDagsByDagId() {
    val dag = DagDef("dag")

    val bundle = Bundle(listOf(dag))

    Assertions.assertEquals(mapOf("dag" to dag), bundle.dags)
  }

  @Test
  @DisplayName("Should reject duplicate dag ids")
  fun shouldRejectDuplicateDagIds() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle(listOf(DagDef("dag"), DagDef("dag")))
      }

    Assertions.assertEquals("Dags in bundle have duplicate ID: dag", error.message)
  }

  @Test
  @DisplayName("Should reject a task depending on an unregistered upstream")
  fun shouldRejectUnregisteredUpstream() {
    val missing = TaskDef("missing", NoOp::class.java)
    val dag = DagDef("dag").addTask(TaskDef("t", NoOp::class.java).dependsOn(missing))

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle(listOf(dag))
      }

    Assertions.assertEquals(
      "Task 't' in Dag 'dag' depends on task 'missing' that is not registered in the same Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a task depending on a task registered in another dag")
  fun shouldRejectUpstreamFromAnotherDag() {
    val foreign = TaskDef("u", NoOp::class.java)
    val other = DagDef("other").addTask(foreign)
    val dag = DagDef("dag").addTask(TaskDef("t", NoOp::class.java).dependsOn(foreign))

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle(listOf(other, dag))
      }

    Assertions.assertEquals(
      "Task 't' in Dag 'dag' depends on task 'u' that is not registered in the same Dag",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject dependency cycles")
  fun shouldRejectDependencyCycle() {
    val a = TaskDef("a", NoOp::class.java)
    val b = TaskDef("b", NoOp::class.java)
    a.dependsOn(b)
    b.dependsOn(a)
    val dag = DagDef("dag").addTask(a).addTask(b)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle(listOf(dag))
      }

    Assertions.assertEquals(
      "Task dependencies in Dag 'dag' contain a cycle involving task 'a'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should accept a diamond-shaped dependency graph")
  fun shouldAcceptDiamondGraph() {
    val root = TaskDef("root", NoOp::class.java)
    val left = TaskDef("left", NoOp::class.java).dependsOn(root)
    val right = TaskDef("right", NoOp::class.java).dependsOn(root)
    val join = TaskDef("join", NoOp::class.java).dependsOn(left, right)
    val dag = DagDef("dag")
    listOf(root, left, right, join).forEach(dag::addTask)

    Assertions.assertEquals(mapOf("dag" to dag), Bundle(listOf(dag)).dags)
  }

  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }
}
