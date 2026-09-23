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
  private class NoOp : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }

  /** A class of handlers that the processor generated [BundleTest_NestedHandlers] for. */
  class Nested

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

  @Test
  @DisplayName("Should leave the bundle unchanged when a Dag fails validation")
  fun shouldNotRegisterInvalidDag() {
    val bundle = Bundle()
    val dag = DagDef("dag").addTask(TaskDef("t", NoOp::class.java).dependsOn(TaskDef("missing", NoOp::class.java)))

    Assertions.assertThrows(IllegalArgumentException::class.java) { bundle.register(dag) }

    Assertions.assertEquals(emptySet<String>(), bundle.dags.keys)
  }

  @Test
  @DisplayName("Should register Dags one at a time and reject a duplicate ID")
  fun shouldRegisterDagsIncrementally() {
    val bundle = Bundle().register(DagDef("a")).register(DagDef("b"))

    Assertions.assertEquals(setOf("a", "b"), bundle.dags.keys)
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) { bundle.register(DagDef("a")) }
    Assertions.assertEquals("Dags in bundle have duplicate ID: a", error.message)
  }

  @Test
  @DisplayName("Should create the Dag on first use when registering a stub-backed handler")
  fun shouldRegisterHandlerAgainstPythonOwnedDag() {
    val bundle =
      Bundle()
        .register("etl", "score", NoopBundleTask::class.java)
        .register("etl", "report", NoopBundleTask::class.java)

    Assertions.assertEquals(setOf("etl"), bundle.dags.keys)
    Assertions.assertEquals(
      setOf("score", "report"),
      bundle.dags
        .getValue("etl")
        .tasks.keys,
    )
  }

  @Test
  @DisplayName("Should find the registrar generated for a nested handler class")
  fun shouldFindRegistrarOfNestedHandlerClass() {
    val bundle = Bundle().register(Nested::class.java)

    Assertions.assertEquals(listOf("etl"), bundle.dags.keys.toList())
    Assertions.assertEquals(
      listOf("score"),
      bundle.dags
        .getValue("etl")
        .tasks.keys
        .toList(),
    )
  }

  @Test
  @DisplayName("Should name the registrar it looked for when there is none")
  fun shouldNameTheRegistrarItLookedFor() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle().register(NoOp::class.java)
      }

    Assertions.assertEquals(
      "No generated registrar org.apache.airflow.sdk.BundleTest_NoOpHandlers for " +
        "${NoOp::class.java.name}; does it carry @Builder.Dag or @Builder.TaskHandler, " +
        "and is airflow-sdk-processor on the annotationProcessor path?",
      error.message,
    )
  }
}

class NoopBundleTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/**
 * Stands in for the registrar the annotation processor generates beside
 * [BundleTest.Nested], to pin the name [Bundle.register] looks up.
 */
@Suppress("ktlint:standard:class-naming", "ClassName")
class BundleTest_NestedHandlers {
  companion object {
    @JvmStatic
    fun registerInto(bundle: Bundle) {
      bundle.register("etl", "score", NoopBundleTask::class.java)
    }
  }
}
