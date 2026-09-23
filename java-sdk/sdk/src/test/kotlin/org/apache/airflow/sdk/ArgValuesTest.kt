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

import org.apache.airflow.sdk.internal.ArgValues
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class ScoreInput : TaskInput {
  /** Pinned, so it binds `region_code` and never folds. */
  @JvmField
  @ArgName("region_code")
  var region: String? = null

  /** Unpinned camelCase: binds `run_label` through the fold. */
  @JvmField
  var runLabel: String? = null

  @JvmField
  var threshold: Double = 0.0

  /** A generic field: its element type has to survive the decode. */
  @JvmField
  var tags: List<String>? = null

  /** Left unbound by every call site below, so it stays null. */
  @JvmField
  var label: String? = null
}

private fun bind(
  bindings: List<Map<String, Any?>>?,
  xcoms: Map<String, Any?> = emptyMap(),
): ScoreInput {
  val (client, _) = clientWith(bindings, xcoms)
  return ArgValues.bindInput(taskContext(), client, ScoreInput::class.java)
}

class FoldedInput : TaskInput {
  @JvmField
  var regionCode: String? = null
}

class CollidingInput : TaskInput {
  @JvmField
  @ArgName("region_code")
  var first: String? = null

  @JvmField
  @ArgName("regionCode")
  var second: String? = null
}

private class CollidingInputTask : InputTask<CollidingInput> {
  override fun execute(
    context: Context,
    client: Client,
    input: CollidingInput,
  ) = Unit
}

private fun bindFolded(bindings: List<Map<String, Any?>>): FoldedInput {
  val (client, _) = clientWith(bindings)
  return ArgValues.bindInput(taskContext(), client, FoldedInput::class.java)
}

internal class ArgValuesTest {
  @Test
  @DisplayName("Should bind fields by their argument name, whatever the call-site order")
  fun shouldBindFieldsByArgName() {
    val input =
      bind(
        listOf(
          mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream"),
          mapOf("kind" to "literal", "name" to "region_code", "value" to "emea"),
        ),
        xcoms = mapOf("upstream" to 0.5),
      )

    assertEquals("emea", input.region)
    assertEquals(0.5, input.threshold)
  }

  @Test
  @DisplayName("Should match a camelCase field to a snake_case argument through the fold")
  fun shouldFoldSnakeCaseArgument() {
    val input = bindFolded(listOf(mapOf("kind" to "literal", "name" to "region_code", "value" to "emea")))

    assertEquals("emea", input.regionCode)
  }

  @Test
  @DisplayName("Should take an @ArgName-pinned field literally rather than folding it")
  fun shouldNotFoldPinnedField() {
    val input =
      bind(
        listOf(
          mapOf("kind" to "literal", "name" to "regionCode", "value" to "emea"),
          mapOf("kind" to "literal", "name" to "run_label", "value" to "nightly"),
          mapOf("kind" to "literal", "name" to "threshold", "value" to 0.5),
        ),
      )

    // 'region' is pinned to region_code, which this call site never bound.
    assertNull(input.region)
    // 'runLabel' is unpinned, so run_label reaches it.
    assertEquals("nightly", input.runLabel)
  }

  @Test
  @DisplayName("Should refuse to guess when two arguments fold to the same name")
  fun shouldRefuseAmbiguousFold() {
    val input =
      bindFolded(
        listOf(
          mapOf("kind" to "literal", "name" to "region_code", "value" to "emea"),
          mapOf("kind" to "literal", "name" to "regioncode", "value" to "apac"),
        ),
      )

    assertNull(input.regionCode)
  }

  @Test
  @DisplayName("Should still bind an exact name that another argument folds onto")
  fun shouldPreferExactNameOverAmbiguousFold() {
    val input =
      bindFolded(
        listOf(
          mapOf("kind" to "literal", "name" to "regionCode", "value" to "emea"),
          mapOf("kind" to "literal", "name" to "region_code", "value" to "apac"),
        ),
      )

    assertEquals("emea", input.regionCode)
  }

  @Test
  @DisplayName("Should reject a TaskInput whose fields claim names that fold alike")
  fun shouldRejectFieldsWithCollidingFolds() {
    val error =
      assertThrows(IllegalArgumentException::class.java) {
        TaskDef("t", CollidingInputTask::class.java)
      }

    assertEquals(
      "TaskInput fields CollidingInput.first and CollidingInput.second claim argument names that " +
        "differ only in case or underscores, which the fold cannot tell apart; rename one of them",
      error.message,
    )
  }

  @Test
  @DisplayName("Should leave a reference field null when the call site bound no argument for it")
  fun shouldLeaveUnboundReferenceFieldNull() {
    val input =
      bind(
        listOf(
          mapOf("kind" to "literal", "name" to "region_code", "value" to "emea"),
          mapOf("kind" to "literal", "name" to "threshold", "value" to 0.5),
        ),
      )

    assertNull(input.label)
  }

  @Test
  @DisplayName("Should keep the element type of a generic field")
  fun shouldKeepGenericFieldElementType() {
    val input =
      bind(
        listOf(
          mapOf("kind" to "literal", "name" to "tags", "value" to listOf("a", "b")),
          mapOf("kind" to "literal", "name" to "threshold", "value" to 0.5),
        ),
      )

    assertEquals(listOf("a", "b"), input.tags)
  }

  @Test
  @DisplayName("Should fail when a primitive field's argument is missing from the call site")
  fun shouldFailOnUnboundPrimitiveField() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        bind(listOf(mapOf("kind" to "literal", "name" to "region_code", "value" to "emea")))
      }

    assertEquals(
      "The stub call bound no argument named 'threshold', required by input field 'threshold'",
      error.message,
    )
  }

  @Test
  @DisplayName("Should name the field when a primitive one is bound to a null literal")
  fun shouldNameFieldBoundToNullLiteral() {
    val error =
      assertThrows(MissingXComException::class.java) {
        bind(listOf(mapOf("kind" to "literal", "name" to "threshold", "value" to null)))
      }

    assertEquals(
      "Task parameter 'threshold' of task 't' is bound to a null literal, but has a primitive type " +
        "that cannot be null; declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should name the upstream when a primitive field's XCom was never pushed")
  fun shouldNameUpstreamForPrimitiveField() {
    val error =
      assertThrows(MissingXComException::class.java) {
        bind(listOf(mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream")))
      }

    assertEquals(
      "Task parameter 'threshold' requires an XCom from task 'upstream', but none was pushed. " +
        "This parameter has a primitive type that cannot be null; declare it with a boxed type " +
        "(e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }
}
