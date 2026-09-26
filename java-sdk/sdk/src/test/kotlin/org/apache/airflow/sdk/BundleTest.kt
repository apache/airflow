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
  @DisplayName("Should find the registrar generated for a nested handler class")
  fun shouldFindRegistrarOfNestedHandlerClass() {
    val bundle = Bundle().register(Nested::class.java)

    val etl = bundle.taskHandlers.getValue("etl")
    Assertions.assertEquals(listOf("etl"), bundle.taskHandlers.keys.toList())
    Assertions.assertEquals(listOf("score"), etl.tasks.keys.toList())
  }

  @Test
  @DisplayName("Should name the registrar it looked for when there is none")
  fun shouldNameTheRegistrarItLookedFor() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle().register(NoOp::class.java)
      }

    Assertions.assertTrue(
      error.message!!.startsWith(
        "No generated registrar org.apache.airflow.sdk.BundleTest_NoOpHandlers for ",
      ),
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a Dag whose ID task handlers already hold")
  fun shouldRejectDagWhoseIdTaskHandlersHold() {
    val bundle = Bundle().register("etl", "score", NoOp::class.java)

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        bundle.register(DagDef("etl"))
      }

    Assertions.assertEquals(
      "Dag 'etl' already has registered task handlers; a Dag declared in Java owns its own " +
        "tasks, so one Dag ID cannot have both",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject task handlers for a Dag ID declared in Java")
  fun shouldRejectTaskHandlersForJavaDeclaredDag() {
    val bundle = Bundle().register(DagDef("etl"))

    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        bundle.register("etl", "score", NoOp::class.java)
      }

    Assertions.assertEquals(
      "Dag 'etl' is declared in Java; attach its tasks with addTask(...) rather than " +
        "registering task handlers for them",
      error.message,
    )
  }

  @Test
  @DisplayName("Should keep accepting more handlers for a Dag the Python file owns")
  fun shouldAcceptMoreHandlersForSameDag() {
    val bundle =
      Bundle()
        .register("etl", "score", NoOp::class.java)
        .register("etl", "report", NoOp::class.java)

    Assertions.assertEquals(
      setOf("score", "report"),
      bundle.taskHandlers
        .getValue("etl")
        .tasks.keys,
    )
  }

  @Test
  @DisplayName("Should find a task whichever side registered its Dag")
  fun shouldFindTaskFromEitherSide() {
    val declared = DagDef("java_etl").addTask("extract", NoOp::class.java)
    val bundle = Bundle().register(declared).register("py_etl", "score", NoOp::class.java)

    Assertions.assertEquals("extract", bundle.taskDef("java_etl", "extract")?.id)
    Assertions.assertEquals("score", bundle.taskDef("py_etl", "score")?.id)
    Assertions.assertNull(bundle.taskDef("java_etl", "score"))
    Assertions.assertNull(bundle.taskDef("absent", "extract"))
  }

  @Test
  @DisplayName("Should reject every register once serving has started")
  fun shouldRejectRegisterAfterServing() {
    val bundle = Bundle()
    bundle.finalizeRegistration()

    val message = "Server.serve has already been called; register everything before serve"
    listOf<() -> Unit>(
      { bundle.register(DagDef("etl")) },
      { bundle.register(Nested::class.java) },
      { bundle.register("etl", "score", NoOp::class.java) },
    ).forEach { register ->
      val error = Assertions.assertThrows(IllegalStateException::class.java, register)
      Assertions.assertEquals(message, error.message)
    }
  }
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
      bundle.register("etl", "score", NoOpHandler::class.java)
    }
  }

  class NoOpHandler : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) = Unit
  }
}
