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

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.In
import org.apache.airflow.sdk.LiteralIn
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskRef
import org.junit.jupiter.api.Assertions.assertEquals
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
    val producer = Refs.register<Long>(dag, TaskDef("p", NoopRefTask::class.java), listOf())
    Refs.register<Unit>(
      dag,
      TaskDef("c", NoopRefTask::class.java),
      listOf(producer, In.value(5)),
    )

    val consumerDef = dag.tasks.getValue("c")
    assertEquals(setOf("p", "c"), dag.tasks.keys)
    assertEquals(setOf(dag.tasks.getValue("p")), consumerDef.upstreams)
    assertEquals(2, consumerDef.inputs.size)
    assertEquals(dag.tasks.getValue("p"), (consumerDef.inputs[0] as TaskRef<*>).def)
    assertEquals(5, (consumerDef.inputs[1] as LiteralIn<*>).value)
  }

  @Test
  @DisplayName("Should pass requireRegistered when every task was wired")
  fun shouldPassRequireRegisteredWhenComplete() {
    val dag = DagDef("d")
    Refs.register<Unit>(dag, TaskDef("t", NoopRefTask::class.java), listOf())

    Refs.requireRegistered(dag, listOf("t"))
  }

  @Test
  @DisplayName("Should fail requireRegistered naming the tasks the wiring missed")
  fun shouldFailRequireRegisteredNamingMissedTasks() {
    val dag = DagDef("d")
    Refs.register<Unit>(dag, TaskDef("t", NoopRefTask::class.java), listOf())

    val error =
      assertThrows(IllegalArgumentException::class.java) {
        Refs.requireRegistered(dag, listOf("t", "x", "y"))
      }

    assertEquals(
      "Wiring for Dag 'd' did not register task(s) 'x', 'y': " +
        "every @Builder.Task method must be invoked in the @Wiring method",
      error.message,
    )
  }
}
