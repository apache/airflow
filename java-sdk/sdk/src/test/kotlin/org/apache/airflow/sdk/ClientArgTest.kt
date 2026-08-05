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

package org.apache.airflow.sdk

import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TIRunContext
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.apache.airflow.sdk.internal.ArgValues
import org.apache.airflow.sdk.internal.Refs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.apache.airflow.sdk.execution.comm.TaskInstance as CommTaskInstance

/** Records getXCom calls and serves canned values keyed by task id. */
private class FakeXComTransport(
  val xcoms: Map<String, Any?> = emptyMap(),
) : org.apache.airflow.sdk.execution.Client {
  val pulls = mutableListOf<Pair<String, Int?>>()

  override fun getConnection(id: String): ConnectionResult = throw NotImplementedError()

  override fun getVariable(key: String): VariableResult = throw NotImplementedError()

  override fun getXCom(
    key: String,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int?,
    includePriorDates: Boolean,
  ): XComResult {
    pulls += taskId to mapIndex
    return XComResult().also {
      it.key = key
      it.value = xcoms[taskId]
    }
  }

  override fun setXCom(
    key: String,
    value: Any,
    dagId: String,
    taskId: String,
    runId: String,
    mapIndex: Int,
  ) = throw NotImplementedError()
}

private fun startupDetails(argBindings: List<Map<String, Any?>>?): StartupDetails =
  StartupDetails().also { details ->
    details.ti =
      CommTaskInstance().also {
        it.dagId = "d"
        it.runId = "r"
        it.taskId = "t"
        it.tryNumber = 1
      }
    details.tiContext = TIRunContext().also { it.argBindings = argBindings }
  }

private fun clientWith(
  argBindings: List<Map<String, Any?>>?,
  xcoms: Map<String, Any?> = emptyMap(),
): Pair<Client, FakeXComTransport> {
  val transport = FakeXComTransport(xcoms)
  return Client(startupDetails(argBindings), transport) to transport
}

private class NoopClientArgTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

private fun taskContext(): Context =
  Context(
    dagRun = DagRun("d", "r", null, null, null, null, null, emptyMap()),
    ti = TaskInstance("d", "r", "t", null, 1),
  )

/** A context whose task was Java-wired with the given inputs. */
private fun contextWiredWith(inputs: List<In<*>>): Context {
  val def = TaskDef("t", NoopClientArgTask::class.java)
  Refs.register<Unit>(DagDef("d"), def, inputs)
  return taskContext().also { it.taskDef = def }
}

internal class ClientArgTest {
  @Test
  @DisplayName("Should resolve a literal binding to its inline value, by position and by name")
  fun shouldResolveLiteralBinding() {
    val (client, transport) = clientWith(listOf(mapOf("kind" to "literal", "name" to "x", "value" to 42L)))

    assertTrue(client.hasArgs())
    assertTrue(client.hasArg(0))
    assertTrue(client.hasArg("x"))
    assertEquals(42L, client.getArg(0))
    assertEquals(42L, client.getArg("x"))
    assertEquals(emptyList<Pair<String, Int?>>(), transport.pulls)
  }

  @Test
  @DisplayName("Should resolve an xcom binding by pulling the bound task's return value")
  fun shouldResolveXComBinding() {
    val (client, transport) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "map_index" to -1L)),
        xcoms = mapOf("upstream" to 7L),
      )

    assertEquals(7L, client.getArg(0))
    assertEquals(listOf("upstream" to null), transport.pulls)
  }

  @Test
  @DisplayName("Should keep bindings in stub-signature order")
  fun shouldKeepBindingOrder() {
    val (client, _) =
      clientWith(
        listOf(
          mapOf("kind" to "literal", "name" to "b", "value" to 2L),
          mapOf("kind" to "literal", "name" to "a", "value" to 1L),
        ),
      )

    assertEquals(2L, client.getArg(0))
    assertEquals(1L, client.getArg(1))
  }

  @Test
  @DisplayName("Should pass a non-negative bound map index to the XCom read")
  fun shouldPassBoundMapIndex() {
    val (client, transport) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "map_index" to 2L)),
        xcoms = mapOf("upstream" to 7L),
      )

    client.getArg(0)

    assertEquals(listOf("upstream" to 2), transport.pulls)
  }

  @Test
  @DisplayName("Should index into a list XCom when the binding has an element index")
  fun shouldResolveElementIndex() {
    val (client, _) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
        xcoms = mapOf("upstream" to listOf("a", "b", "c")),
      )

    assertEquals("b", client.getArg(0))
  }

  @Test
  @DisplayName("Should fail when an element index points into a non-list XCom")
  fun shouldRejectElementIndexOnNonList() {
    val (client, _) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
        xcoms = mapOf("upstream" to "scalar"),
      )

    assertThrows(IllegalStateException::class.java) { client.getArg(0) }
  }

  @Test
  @DisplayName("Should reject reading an argument that was never bound")
  fun shouldRejectUnknownArg() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "literal", "name" to "x", "value" to 1L)))

    assertFalse(client.hasArg("y"))
    assertFalse(client.hasArg(1))
    assertThrows(IllegalArgumentException::class.java) { client.getArg("y") }
    assertThrows(IllegalArgumentException::class.java) { client.getArg(1) }
  }

  @Test
  @DisplayName("Should report no bound arguments when the supervisor sent none")
  fun shouldHandleAbsentBindings() {
    val (client, _) = clientWith(null)

    assertFalse(client.hasArgs())
    assertFalse(client.hasArg("x"))
    assertFalse(client.hasArg(0))
  }

  @Test
  @DisplayName("Should fail on an unsupported binding kind")
  fun shouldRejectUnknownBindingKind() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "mystery", "name" to "x")))

    assertThrows(IllegalStateException::class.java) { client.hasArg("x") }
  }

  @Test
  @DisplayName("Should fail on duplicate binding names")
  fun shouldRejectDuplicateBindingNames() {
    val (client, _) =
      clientWith(
        listOf(
          mapOf("kind" to "literal", "name" to "x", "value" to 1L),
          mapOf("kind" to "literal", "name" to "x", "value" to 2L),
        ),
      )

    assertThrows(IllegalStateException::class.java) { client.hasArgs() }
  }

  @Test
  @DisplayName("Should resolve a flat data parameter from the binding at its position")
  fun shouldResolvePositionalBinding() {
    val (client, transport) =
      clientWith(
        listOf(
          mapOf("kind" to "literal", "name" to "first", "value" to 5L),
          mapOf("kind" to "xcom", "name" to "second", "task_id" to "upstream"),
        ),
        xcoms = mapOf("upstream" to "pulled"),
      )

    assertEquals(5, ArgValues.requiredInput(taskContext(), client, 0, Integer::class.java, "first").toInt())
    assertEquals("pulled", ArgValues.optionalInput(taskContext(), client, 1, String::class.java))
    assertEquals(listOf("upstream" to null), transport.pulls)
  }

  @Test
  @DisplayName("Should prefer the runtime binding at the position over the Java wiring")
  fun shouldPreferRuntimeBinding() {
    val context = contextWiredWith(listOf(In.value(9L)))
    val (client, transport) = clientWith(listOf(mapOf("kind" to "literal", "name" to "value", "value" to 5L)))

    val resolved = ArgValues.requiredInput(context, client, 0, Integer::class.java, "value")

    assertEquals(5, resolved.toInt())
    assertEquals(emptyList<Pair<String, Int?>>(), transport.pulls)
  }

  @Test
  @DisplayName("Should fall back to the Java wiring without runtime bindings")
  fun shouldFallBackToWiring() {
    val context = contextWiredWith(listOf(In.value(9L)))
    val (client, _) = clientWith(null)

    assertFalse(ArgValues.hasRuntimeBindings(client))
    assertEquals(9, ArgValues.requiredInput(context, client, 0, Integer::class.java, "value").toInt())
  }

  @Test
  @DisplayName("Should fail fast when the stub call bound fewer arguments than declared")
  fun shouldFailOnArityMismatch() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "literal", "name" to "only", "value" to 1L)))

    val error =
      assertThrows(IllegalStateException::class.java) {
        ArgValues.optionalInput(taskContext(), client, 1, Integer::class.java)
      }

    assertEquals(
      "Task 't' declares a data parameter at position 1 but the stub call bound only 1 argument(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should throw MissingXComException for a required argument bound to a null literal")
  fun shouldThrowForNullLiteralOnRequired() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "literal", "name" to "value", "value" to null)))

    assertThrows(MissingXComException::class.java) {
      ArgValues.requiredInput(taskContext(), client, 0, Integer::class.java, "value")
    }
  }

  @Test
  @DisplayName("Should resolve input-bundle fields by wire name")
  fun shouldResolveNamedBindings() {
    val (client, _) =
      clientWith(
        listOf(
          mapOf("kind" to "literal", "name" to "region_code", "value" to "emea"),
          mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream"),
        ),
        xcoms = mapOf("upstream" to 0.5),
      )

    assertTrue(ArgValues.hasRuntimeBindings(client))
    assertEquals("emea", ArgValues.optionalNamed(client, "region_code", String::class.java))
    assertEquals(0.5, ArgValues.requiredNamed(client, "threshold", java.lang.Double::class.java, "threshold"))
  }

  @Test
  @DisplayName("Should resolve an absent named binding to null for optional fields and fail for required ones")
  fun shouldHandleAbsentNamedBinding() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "literal", "name" to "other", "value" to 1L)))

    assertNull(ArgValues.optionalNamed(client, "missing", String::class.java))
    val error =
      assertThrows(IllegalStateException::class.java) {
        ArgValues.requiredNamed(client, "missing", Integer::class.java, "field")
      }
    assertEquals(
      "The stub call bound no argument named 'missing', required by input field 'field'",
      error.message,
    )
  }
}
