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

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.Deps
import org.apache.airflow.sdk.LiteralArg
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/** Stands in for the generated wiring view of a task group. */
private fun groupView(id: String) =
  object : Deps.TaskGroup {
    override fun groupId() = id
  }

private class NoopRefTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

internal class RefsTest {
  @Test
  @DisplayName("Should register the task, record inputs, and wire handle edges")
  fun shouldRegisterTaskWithInputsAndEdges() {
    val dag = DagDef("d")
    Refs.record(dag, listOf("p", "c"), emptyList()) {
      val producer = Refs.node<Long>("", TaskDef("p", NoopRefTask::class.java))
      Refs.call<Unit>("", TaskDef("c", NoopRefTask::class.java), producer, Arg.lit(5))
    }

    val consumerDef = dag.tasks.getValue("c")
    assertEquals(setOf("p", "c"), dag.tasks.keys)
    assertEquals(setOf(dag.tasks.getValue("p")), consumerDef.upstreams)
    assertEquals(2, consumerDef.inputs.size)
    assertEquals(dag.tasks.getValue("p"), (consumerDef.inputs[0] as TaskRef<*>).def)
    assertEquals(5, (consumerDef.inputs[1] as LiteralArg<*>).value)
  }

  @Test
  @DisplayName("Should return the same handle wherever a task is wired")
  fun shouldMemoizeHandleByTaskId() {
    val dag = DagDef("d")
    Refs.record(dag, listOf("a", "b"), emptyList()) {
      val first = Refs.node<Unit>("", TaskDef("a", NoopRefTask::class.java))
      val again = Refs.node<Unit>("", TaskDef("a", NoopRefTask::class.java))
      assertSame(first, again)
      first.before(Refs.node<Unit>("", TaskDef("b", NoopRefTask::class.java)))
    }

    assertEquals(setOf("a", "b"), dag.tasks.keys)
    assertEquals(setOf(dag.tasks.getValue("a")), dag.tasks.getValue("b").upstreams)
  }

  @Test
  @DisplayName("Should pass when the wiring registered every task")
  fun shouldPassWhenWiringComplete() {
    val dag = DagDef("d")

    Refs.record(dag, listOf("t"), emptyList()) { Refs.node<Unit>("", TaskDef("t", NoopRefTask::class.java)) }
  }

  @Test
  @DisplayName("Should fail naming the tasks the wiring missed")
  fun shouldFailNamingMissedTasks() {
    val dag = DagDef("d")

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.record(dag, listOf("t", "x", "y"), emptyList()) { Refs.node<Unit>("", TaskDef("t", NoopRefTask::class.java)) }
      }

    assertEquals(
      "Wiring for Dag 'd' did not register task(s) 'x', 'y': " +
        "every @Builder.Task method must be called in the @Builder.Deps class",
      error.message,
    )
  }

  @Test
  @DisplayName("Should refuse a wiring call made outside a recording")
  fun shouldRefuseWiringOutsideRecording() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        Refs.node<Unit>("", TaskDef("t", NoopRefTask::class.java))
      }

    assertEquals(
      "Task 't' was wired outside a @Builder.Deps class; the wiring view's methods " +
        "only record while the generated builder is running depends()",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a raw null argument, pointing to lit(null)")
  fun shouldRejectRawNullArgument() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.record(DagDef("d"), listOf("t"), emptyList()) {
          Refs.call<Unit>("", TaskDef("t", NoopRefTask::class.java), Arg.lit(1), null)
        }
      }

    assertEquals("Argument 2 of task 't' is null; wrap a null constant as lit(null)", error.message)
  }

  @Test
  @DisplayName("Should reject a task wired a second time with arguments")
  fun shouldRejectTaskWiredTwiceWithArguments() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.record(DagDef("d"), listOf("t"), emptyList()) {
          Refs.node<Unit>("", TaskDef("t", NoopRefTask::class.java))
          Refs.call<Unit>("", TaskDef("t", NoopRefTask::class.java), Arg.lit(1))
        }
      }

    assertEquals(
      "Task 't' is wired more than once with arguments; call it once and reuse the handle it returned",
      error.message,
    )
  }

  @Test
  @DisplayName("Should refuse to record a Dag while another is being recorded")
  fun shouldRefuseNestedRecording() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        Refs.record(DagDef("outer"), emptyList(), emptyList()) {
          Refs.record(DagDef("inner"), emptyList(), emptyList()) {}
        }
      }

    assertEquals("Dag wiring is already being recorded on this thread", error.message)
  }

  @Test
  @DisplayName("Should make every group before the wiring runs and register each task in its own")
  fun shouldRegisterGroupedTaskInGroup() {
    val dag = DagDef("d")
    Refs.record(
      dag,
      listOf("extract", "staging.checks.nulls"),
      listOf("staging", "staging.checks", "staging.empty"),
    ) {
      val extract = Refs.node<Unit>("", TaskDef("extract", NoopRefTask::class.java))
      extract.before(groupView("staging"))
      Refs.node<Unit>("staging.checks", TaskDef("staging.checks.nulls", NoopRefTask::class.java))
    }

    // staging.empty holds no task, so only the group list can have made it.
    assertEquals(listOf("staging", "staging.checks", "staging.empty"), dag.groups.keys.toList())
    assertEquals(listOf("staging.checks.nulls"), dag.groups.getValue("staging.checks").taskIds)
    assertEquals(1, dag.groupEdges.size)
  }

  @Test
  @DisplayName("Should resolve the group a wiring-view group stands for")
  fun shouldResolveGroupOfView() {
    val dag = DagDef("d")
    Refs.record(dag, listOf("staging.stage"), listOf("staging")) {
      Refs.node<Unit>("staging", TaskDef("staging.stage", NoopRefTask::class.java))
      assertEquals(listOf("staging.stage"), groupView("staging").nodes().map { it.id })
    }
  }

  @Test
  @DisplayName("Should fail naming a group the Dag does not have")
  fun shouldFailOnUnknownGroup() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.record(DagDef("d"), emptyList(), emptyList()) { Refs.group("staging") }
      }

    assertEquals("Dag 'd' has no task group 'staging'", error.message)
  }
}
