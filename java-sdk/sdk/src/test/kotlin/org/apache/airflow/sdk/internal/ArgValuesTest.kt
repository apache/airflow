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

import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.DagDef
import org.apache.airflow.sdk.DagRun
import org.apache.airflow.sdk.LiteralArg
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskInstance
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.execution.AssetRef
import org.apache.airflow.sdk.execution.comm.AssetStateStoreResult
import org.apache.airflow.sdk.execution.comm.ConnectionResult
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TaskStateStoreResult
import org.apache.airflow.sdk.execution.comm.VariableResult
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID
import org.apache.airflow.sdk.execution.Client as Transport
import org.apache.airflow.sdk.execution.comm.TaskInstance as CommTaskInstance

private class NoopArgTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** Resolution of the inputs a `@Builder.Deps` class recorded, without runtime bindings. */
internal class ArgValuesTest {
  /** Upstream task ids read through the transport, in arrival order. */
  private val pulls = java.util.concurrent.CopyOnWriteArrayList<String>()

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

        override fun setVariable(
          key: String,
          value: String,
          description: String?,
        ): Unit = throw NotImplementedError()

        override fun deleteVariable(key: String): Unit = throw NotImplementedError()

        override fun getXCom(
          key: String,
          dagId: String,
          taskId: String,
          runId: String,
          mapIndex: Int?,
          includePriorDates: Boolean,
        ): XComResult {
          pulls += taskId
          arrival?.let {
            it.countDown()
            check(it.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
              "wired upstreams were read one at a time"
            }
          }
          return XComResult().also { it.value = xcomsByTask[taskId] }
        }

        override fun setXCom(
          key: String,
          value: Any,
          dagId: String,
          taskId: String,
          runId: String,
          mapIndex: Int,
        ): Unit = throw NotImplementedError()

        override fun getTaskStateStore(
          tiId: UUID,
          key: String,
        ): TaskStateStoreResult? = throw NotImplementedError()

        override fun setTaskStateStore(
          tiId: UUID,
          key: String,
          value: Any,
          expiresAt: OffsetDateTime?,
        ): Unit = throw NotImplementedError()

        override fun deleteTaskStateStore(
          tiId: UUID,
          key: String,
        ): Unit = throw NotImplementedError()

        override fun clearTaskStateStore(tiId: UUID): Unit = throw NotImplementedError()

        override fun getAssetStateStore(
          asset: AssetRef,
          key: String,
        ): AssetStateStoreResult? = throw NotImplementedError()

        override fun setAssetStateStore(
          asset: AssetRef,
          key: String,
          value: Any,
        ): Unit = throw NotImplementedError()

        override fun deleteAssetStateStore(
          asset: AssetRef,
          key: String,
        ): Unit = throw NotImplementedError()

        override fun clearAssetStateStore(asset: AssetRef): Unit = throw NotImplementedError()
      },
    )

  private var arrival: java.util.concurrent.CountDownLatch? = null

  private fun contextFor(inputs: List<Arg<*>>): Context {
    val dag = DagDef("d")
    inputs
      .filterIsInstance<TaskRef<*>>()
      .map { it.def }
      .distinct()
      .forEach { dag.addTask(it) }
    val def = TaskDef("consumer", NoopArgTask::class.java)
    Refs.record(dag, listOf("consumer"), emptyList()) { Refs.call<Unit>("", def, emptyList(), *inputs.toTypedArray()) }
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

    val args = TaskArgs.of(context, clientWith(mapOf("producer" to 42L)), 1)

    assertEquals(42L, args.require(0, java.lang.Long::class.java))
  }

  @Test
  @DisplayName("Should resolve a literal input without touching the client")
  fun shouldResolveLiteralInput() {
    val context = contextFor(listOf(LiteralArg(7)))

    val args = TaskArgs.of(context, clientWith(emptyMap()), 1)

    assertEquals(7L, args.require(0, java.lang.Long::class.java))
  }

  @Test
  @DisplayName("Should throw MissingXComException when a required upstream pushed no value")
  fun shouldThrowForMissingRequiredValue() {
    val context = contextFor(listOf(handleFor("producer")))
    val client = clientWith(mapOf("producer" to null))

    val args = TaskArgs.of(context, client, 1)

    assertThrows(MissingXComException::class.java) { args.require(0, Integer::class.java) }
  }

  @Test
  @DisplayName("Should throw MissingXComException for a required null literal")
  fun shouldThrowForRequiredNullLiteral() {
    val context = contextFor(listOf(LiteralArg<Int>(null)))
    val client = clientWith(emptyMap())

    val args = TaskArgs.of(context, client, 1)

    val error = assertThrows(MissingXComException::class.java) { args.require(0, Integer::class.java) }

    assertEquals(
      "Task parameter '#0' is wired to a null literal, but has a primitive type that cannot " +
        "be null; declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should pass null through for optional inputs")
  fun shouldPassNullThroughForOptionalInputs() {
    val context = contextFor(listOf(handleFor("producer")))

    val args = TaskArgs.of(context, clientWith(mapOf("producer" to null)), 1)

    assertNull(args.get(0, Integer::class.java))
  }

  @Test
  @DisplayName("Should fail when the Dag wired fewer inputs than the task declares")
  fun shouldFailOnUnwiredPosition() {
    val context = contextFor(listOf(handleFor("producer")))

    val error =
      assertThrows(IllegalStateException::class.java) {
        TaskArgs.of(context, clientWith(emptyMap()), 2)
      }

    assertEquals(
      "Task 'consumer' declares 2 data parameter(s) but the Dag wired 1 argument(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should read an upstream once when several parameters are wired to the same handle")
  fun shouldReadEachUpstreamOnce() {
    val producer = handleFor("producer")
    val context = contextFor(listOf(producer, producer))

    val args = TaskArgs.of(context, clientWith(mapOf("producer" to 42L)), 2)

    assertEquals(42L, args.require(0, java.lang.Long::class.java))
    assertEquals(42L, args.require(1, java.lang.Long::class.java))
    assertEquals(listOf("producer"), pulls)
  }

  @Test
  @DisplayName("Should read wired upstreams concurrently rather than one at a time")
  fun shouldReadWiredUpstreamsConcurrently() {
    val context = contextFor(listOf(handleFor("left"), handleFor("right")))
    // Each read blocks until both have arrived, so sequential resolution
    // cannot satisfy it and the check inside the transport fails.
    arrival = java.util.concurrent.CountDownLatch(2)

    val args = TaskArgs.of(context, clientWith(mapOf("left" to 1L, "right" to 2L)), 2)

    assertEquals(1L, args.require(0, java.lang.Long::class.java))
    assertEquals(2L, args.require(1, java.lang.Long::class.java))
    assertEquals(setOf("left", "right"), pulls.toSet())
  }

  @Test
  @DisplayName("Should fail when the Dag wired more inputs than the task declares")
  fun shouldFailOnSurplusWiredInput() {
    val context = contextFor(listOf(handleFor("left"), handleFor("right")))

    val error =
      assertThrows(IllegalStateException::class.java) {
        TaskArgs.of(context, clientWith(emptyMap()), 1)
      }

    assertEquals(
      "Task 'consumer' declares 1 data parameter(s) but the Dag wired 2 argument(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should report the stub call when nothing was wired and nothing was bound")
  fun shouldFailWithoutTaskDef() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        TaskArgs.of(contextWithoutTaskDef(), clientWith(emptyMap()), 1)
      }

    assertEquals(
      "Task 'consumer' declares 1 data parameter(s) but the stub call bound 0 argument(s)",
      error.message,
    )
  }
}
