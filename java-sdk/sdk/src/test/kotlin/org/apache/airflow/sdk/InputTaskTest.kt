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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/** The bindings a keyword call site delivers for [SummaryInput]. */
private val NAMED_BINDINGS =
  listOf(
    mapOf("kind" to "literal", "name" to "region_code", "value" to "emea"),
    mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream"),
  )

class SummaryInput : TaskInput {
  @JvmField
  @ArgName("region_code")
  var region: String? = null

  @JvmField
  var threshold: Double = 0.0

  /** Left unbound by [NAMED_BINDINGS], so it stays null. */
  @JvmField
  var label: String? = null
}

private class Summarize : InputTask<SummaryInput> {
  var received: SummaryInput? = null

  override fun execute(
    context: Context,
    client: Client,
    input: SummaryInput,
  ) {
    received = input
  }
}

private abstract class SummarizeBase : InputTask<SummaryInput> {
  var received: SummaryInput? = null
}

private class InheritedSummarize : SummarizeBase() {
  override fun execute(
    context: Context,
    client: Client,
    input: SummaryInput,
  ) {
    received = input
  }
}

private class Unresolved<I : TaskInput> : InputTask<I> {
  override fun execute(
    context: Context,
    client: Client,
    input: I,
  ) = Unit
}

class HiddenFieldInput : TaskInput {
  var hidden: String? = null
}

private class HiddenFieldTask : InputTask<HiddenFieldInput> {
  override fun execute(
    context: Context,
    client: Client,
    input: HiddenFieldInput,
  ) = Unit
}

class ConstructedInput(
  @JvmField val value: String,
) : TaskInput

private class ConstructedInputTask : InputTask<ConstructedInput> {
  override fun execute(
    context: Context,
    client: Client,
    input: ConstructedInput,
  ) = Unit
}

internal class InputTaskTest {
  @Test
  @DisplayName("Should inject a TaskInput whose fields are bound by argument name")
  fun shouldInjectNamedTaskInput() {
    val (client, _) = clientWith(NAMED_BINDINGS, xcoms = mapOf("upstream" to 0.5))
    val task = Summarize()

    task.execute(taskContext(), client)

    val input = requireNotNull(task.received)
    assertEquals("emea", input.region)
    assertEquals(0.5, input.threshold)
    assertNull(input.label)
  }

  @Test
  @DisplayName("Should decode a TaskInput wholesale from its wired input when no bindings arrive")
  fun shouldDecodeTaskInputFromWiredInput() {
    // A native Dag has no stub call site, so there are no argument names to
    // match fields against: the input the Dag wired to this task decodes into
    // the whole TaskInput at once.
    val context = contextWiredWith(listOf(LiteralArg(mapOf("region" to "emea", "threshold" to 0.5))))
    val (client, _) = clientWith(null)
    val task = Summarize()

    task.execute(context, client)

    val input = requireNotNull(task.received)
    assertEquals("emea", input.region)
    assertEquals(0.5, input.threshold)
  }

  @Test
  @DisplayName("Should fail when the input wired to a TaskInput resolves to nothing")
  fun shouldRejectNullWiredTaskInput() {
    val context = contextWiredWith(listOf(LiteralArg<Map<String, Any?>>(null)))
    val (client, _) = clientWith(null)

    val error =
      assertThrows(MissingXComException::class.java) { Summarize().execute(context, client) }

    assertEquals(
      "Input 'SummaryInput' is wired to a null literal, so there is nothing to bind.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should still match a TaskInput by name when the stub call bound no arguments")
  fun shouldBindTaskInputWhenStubBoundNothing() {
    val (client, _) = clientWith(null)

    val error =
      assertThrows(IllegalStateException::class.java) { Summarize().execute(taskContext(), client) }

    assertEquals(
      "The stub call bound no argument named 'threshold', required by input field 'threshold'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should resolve the input type a superclass declared")
  fun shouldResolveInheritedInputType() {
    val (client, _) = clientWith(NAMED_BINDINGS, xcoms = mapOf("upstream" to 0.5))
    val task = InheritedSummarize()

    task.execute(taskContext(), client)

    assertEquals("emea", requireNotNull(task.received).region)
  }

  @Test
  @DisplayName("Should reject registering a task whose input type cannot be resolved")
  fun shouldRejectUnresolvableInputType() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TaskDef("t", Unresolved::class.java)
      }

    assertEquals(
      "Task class ${Unresolved::class.java.name} implements InputTask with an input type that cannot be " +
        "resolved; declare a concrete type argument, e.g. 'implements InputTask<MyInput>'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject registering a task whose TaskInput field cannot be assigned")
  fun shouldRejectUnassignableField() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TaskDef("t", HiddenFieldTask::class.java)
      }

    assertEquals(
      "TaskInput field HiddenFieldInput.hidden must be public and non-final so the SDK can assign its binding",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject registering a task whose TaskInput has no public no-argument constructor")
  fun shouldRejectTaskInputWithoutNoArgConstructor() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TaskDef("t", ConstructedInputTask::class.java)
      }

    assertEquals("TaskInput class ConstructedInput needs a public no-argument constructor", error.message)
  }
}
