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
import org.apache.airflow.sdk.LiteralArg
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

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
    Refs.record(dag, listOf("p", "c")) {
      val producer = Refs.node<Long>(TaskDef("p", NoopRefTask::class.java))
      Refs.call<Unit>(TaskDef("c", NoopRefTask::class.java), producer, Arg.lit(5))
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
    Refs.record(dag, listOf("a", "b")) {
      val first = Refs.node<Unit>(TaskDef("a", NoopRefTask::class.java))
      val again = Refs.node<Unit>(TaskDef("a", NoopRefTask::class.java))
      assertSame(first, again)
      first.then(Refs.node<Unit>(TaskDef("b", NoopRefTask::class.java)))
    }

    assertEquals(setOf("a", "b"), dag.tasks.keys)
    assertEquals(setOf(dag.tasks.getValue("a")), dag.tasks.getValue("b").upstreams)
  }

  @Test
  @DisplayName("Should pass when the wiring registered every task")
  fun shouldPassWhenWiringComplete() {
    val dag = DagDef("d")

    Refs.record(dag, listOf("t")) { Refs.node<Unit>(TaskDef("t", NoopRefTask::class.java)) }
  }

  @Test
  @DisplayName("Should fail naming the tasks the wiring missed")
  fun shouldFailNamingMissedTasks() {
    val dag = DagDef("d")

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.record(dag, listOf("t", "x", "y")) { Refs.node<Unit>(TaskDef("t", NoopRefTask::class.java)) }
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
        Refs.node<Unit>(TaskDef("t", NoopRefTask::class.java))
      }

    assertEquals(
      "Task 't' was wired outside a @Builder.Deps class; the wiring view's methods " +
        "only record while the generated builder is running depends()",
      error.message,
    )
  }
}
