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

@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.DagRun
import org.apache.airflow.sdk.In
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskInstance
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.apache.airflow.sdk.execution.Client as Transport
import org.apache.airflow.sdk.execution.comm.TaskInstance as CommTaskInstance

private class NoopArgTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** Resolution of the inputs a `@Wiring` method recorded, without runtime bindings. */
internal class ArgValuesTest {
  private fun clientWith(xcomsByTask: Map<String, Any?>): Client =
    Client(
      StartupDetails().also {
        it.ti =
          CommTaskInstance().also { ti ->
            ti.taskId = "consumer"
            ti.dagId = "d"
            ti.runId = "r"
            ti.tryNumber = 1
          }
      },
      object : Transport {
        override fun getConnection(id: String): ConnectionResult = throw NotImplementedError()

        override fun getVariable(key: String): VariableResult = throw NotImplementedError()

        override fun getXCom(
          key: String,
          dagId: String,
          taskId: String,
          runId: String,
          mapIndex: Int?,
          includePriorDates: Boolean,
        ): XComResult = XComResult().also { it.value = xcomsByTask[taskId] }

        override fun setXCom(
          key: String,
          value: Any,
          dagId: String,
          taskId: String,
          runId: String,
          mapIndex: Int,
        ): Unit = throw NotImplementedError()
      },
    )

  private fun contextFor(inputs: List<In<*>>): Context {
    val dag = DagDef("d")
    inputs.filterIsInstance<TaskRef<*>>().forEach { dag.addTask(it.def) }
    val def = TaskDef("consumer", NoopArgTask::class.java)
    Refs.register<Unit>(dag, def, inputs)
    return contextWithoutTaskDef().also { it.taskDef = def }
  }

  private fun contextWithoutTaskDef(): Context =
    Context(
      dagRun = DagRun("d", "r", null, null, null, null, null, emptyMap()),
      ti = TaskInstance("d", "r", "consumer", null, 1),
    )

  private fun handleFor(taskId: String): TaskRef<Any> = TaskRef(TaskDef(taskId, NoopArgTask::class.java))

  @Test
  @DisplayName("Should resolve a handle input from the upstream task's XCom")
  fun shouldResolveHandleInputFromXCom() {
    val context = contextFor(listOf(handleFor("producer")))

    assertEquals(
      42L,
      ArgValues.requiredInput(context, clientWith(mapOf("producer" to 42L)), 0, java.lang.Long::class.java, "value"),
    )
  }

  @Test
  @DisplayName("Should resolve a literal input without touching the client")
  fun shouldResolveLiteralInput() {
    val context = contextFor(listOf(In.value(7)))

    assertEquals(7L, ArgValues.requiredInput(context, clientWith(emptyMap()), 0, java.lang.Long::class.java, "value"))
  }

  @Test
  @DisplayName("Should throw MissingXComException when a required upstream pushed no value")
  fun shouldThrowForMissingRequiredValue() {
    val context = contextFor(listOf(handleFor("producer")))
    val client = clientWith(mapOf("producer" to null))

    assertThrows(MissingXComException::class.java) {
      ArgValues.requiredInput(context, client, 0, Integer::class.java, "value")
    }
  }

  @Test
  @DisplayName("Should throw MissingXComException for a required null literal")
  fun shouldThrowForRequiredNullLiteral() {
    val context = contextFor(listOf(In.value<Int>(null)))
    val client = clientWith(emptyMap())

    val error =
      assertThrows(MissingXComException::class.java) {
        ArgValues.requiredInput(context, client, 0, Integer::class.java, "value")
      }

    assertEquals(
      "'value' has a primitive type but its wired literal input is null; " +
        "declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should pass null through for optional inputs")
  fun shouldPassNullThroughForOptionalInputs() {
    val context = contextFor(listOf(handleFor("producer")))

    assertNull(ArgValues.optionalInput(context, clientWith(mapOf("producer" to null)), 0, Integer::class.java))
  }

  @Test
  @DisplayName("Should fail when the position has no wired input")
  fun shouldFailOnUnwiredPosition() {
    val context = contextFor(listOf())

    val error =
      assertThrows(IllegalStateException::class.java) {
        ArgValues.optionalInput(context, clientWith(emptyMap()), 0, Integer::class.java)
      }

    assertEquals(
      "Task 'consumer' declares a data parameter at position 0 but only 0 input(s) are wired",
      error.message,
    )
  }

  @Test
  @DisplayName("Should fail when the context carries no task definition")
  fun shouldFailWithoutTaskDef() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        ArgValues.optionalInput(contextWithoutTaskDef(), clientWith(emptyMap()), 0, Integer::class.java)
      }

    assertEquals(
      "Task 'consumer' declares data parameters but has no wired inputs; " +
        "register it through a @Wiring method",
      error.message,
    )
  }
}
