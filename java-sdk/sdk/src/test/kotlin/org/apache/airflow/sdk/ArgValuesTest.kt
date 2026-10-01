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

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.LogSender
import org.apache.airflow.sdk.internal.ArgValues
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
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
}

class FoldedInput : TaskInput {
  @JvmField
  var regionCode: String? = null
}

/** One primitive field, so resolving to nothing is the only failure possible. */
class ThresholdInput : TaskInput {
  @JvmField
  var threshold: Double = 0.0
}

/** The boxed twin of [ThresholdInput], which can take the null. */
class BoxedThresholdInput : TaskInput {
  @JvmField
  var threshold: Double? = null
}

/** A generic field: its element type has to survive the decode. */
class TagsInput : TaskInput {
  @JvmField
  var tags: List<String>? = null
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

private fun <I : TaskInput> bind(
  type: Class<I>,
  bindings: List<Map<String, Any?>>,
  xcoms: Map<String, Any?> = emptyMap(),
): I {
  val (client, _) = clientWith(bindings, xcoms)
  return ArgValues.bindInput(client, type)
}

private fun literal(
  name: String,
  value: Any?,
  fromDefault: Boolean = false,
): Map<String, Any?> = mapOf("kind" to "literal", "name" to name, "value" to value, "from_default" to fromDefault)

internal class ArgValuesTest {
  @Test
  @DisplayName("Should bind fields by their argument name, whatever the call-site order")
  fun shouldBindFieldsByArgName() {
    val input =
      bind(
        ScoreInput::class.java,
        listOf(
          mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream"),
          literal("run_label", "nightly"),
          literal("region_code", "emea"),
        ),
        xcoms = mapOf("upstream" to 0.5),
      )

    assertEquals("emea", input.region)
    assertEquals("nightly", input.runLabel)
    assertEquals(0.5, input.threshold)
  }

  @Test
  @DisplayName("Should match a camelCase field to a snake_case argument through the fold")
  fun shouldFoldSnakeCaseArgument() {
    val input = bind(FoldedInput::class.java, listOf(literal("region_code", "emea")))

    assertEquals("emea", input.regionCode)
  }

  @Test
  @DisplayName("Should keep the element type of a generic field")
  fun shouldKeepGenericFieldElementType() {
    val input = bind(TagsInput::class.java, listOf(literal("tags", listOf("a", "b"))))

    assertEquals(listOf("a", "b"), input.tags)
  }

  @Test
  @DisplayName("Should still bind an exact name that another argument folds onto")
  fun shouldPreferExactNameOverAmbiguousFold() {
    val input =
      bind(
        FoldedInput::class.java,
        listOf(literal("regionCode", "emea"), literal("region_code", "apac")),
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
  @DisplayName("Should warn in each direction at once and default the unfilled field")
  fun shouldWarnOnFieldMatchingNoArgument() {
    LogSender.messages.clear()

    // The field and the argument miss each other, so both directions report.
    val input = bind(FoldedInput::class.java, listOf(literal("threshold", 0.5)))

    assertNull(input.regionCode)
    val unfilled = LogSender.messages.single { it.event == "Task handler declares argument(s) the Dag's call did not pass" }
    assertEquals(listOf("regionCode (argument 'regionCode')"), unfilled.arguments["declared_not_passed"])
    assertEquals(listOf("threshold"), unfilled.arguments["passed"])
    val unclaimed = LogSender.messages.single { it.event == "Dag's call passed argument(s) the task handler does not declare" }
    assertEquals(listOf("threshold"), unclaimed.arguments["passed_not_declared"])
    assertEquals(listOf("regionCode"), unclaimed.arguments["declared"])
  }

  @Test
  @DisplayName("Should warn when an @ArgName-pinned name is not among the arguments")
  fun shouldWarnWhenPinnedNameDoesNotFold() {
    LogSender.messages.clear()

    // 'region' is pinned to region_code, which the camelCase argument cannot reach.
    val input =
      bind(
        ScoreInput::class.java,
        listOf(
          literal("regionCode", "emea"),
          literal("run_label", "nightly"),
          literal("threshold", 0.5),
        ),
      )

    assertNull(input.region)
    assertEquals("nightly", input.runLabel)
    val unfilled = LogSender.messages.single { it.event == "Task handler declares argument(s) the Dag's call did not pass" }
    assertEquals(listOf("region (argument 'region_code')"), unfilled.arguments["declared_not_passed"])
  }

  @Test
  @DisplayName("Should warn rather than guess when two arguments fold to the field's name")
  fun shouldWarnOnAmbiguousFold() {
    LogSender.messages.clear()

    val input =
      bind(
        FoldedInput::class.java,
        listOf(literal("region_code", "emea"), literal("regioncode", "apac")),
      )

    assertNull(input.regionCode)
    val message = LogSender.messages.single { it.event == "Task handler declares argument(s) the Dag's call did not pass" }
    assertEquals(
      listOf(
        "regionCode (argument 'regionCode' matches more than one passed argument differing only " +
          "in case or underscores; add @ArgName)",
      ),
      message.arguments["declared_not_passed"],
    )
  }

  @Test
  @DisplayName("Should bind and warn when the call site passes an argument no field claims")
  fun shouldWarnOnUnclaimedArgument() {
    LogSender.messages.clear()

    val input =
      bind(
        FoldedInput::class.java,
        listOf(literal("region_code", "emea"), literal("extra", 1L)),
      )

    assertEquals("emea", input.regionCode)
    val message = LogSender.messages.single { it.level == Level.WARNING }
    assertEquals("Dag's call passed argument(s) the task handler does not declare", message.event)
    assertEquals(listOf("extra"), message.arguments["passed_not_declared"])
    assertEquals(listOf("regionCode"), message.arguments["declared"])
    assertEquals("FoldedInput", message.arguments["input"])
  }

  @Test
  @DisplayName("Should stay quiet about an unclaimed argument the call site never passed")
  fun shouldNotWarnOnUnclaimedCapturedDefault() {
    LogSender.messages.clear()

    bind(
      FoldedInput::class.java,
      listOf(literal("region_code", "emea"), literal("extra", 1L, fromDefault = true)),
    )

    assertTrue(LogSender.messages.none { it.level == Level.WARNING }) {
      "unexpected warnings: ${LogSender.messages.map { it.event }}"
    }
  }

  @Test
  @DisplayName("Should give a boxed field null when its argument resolves to nothing")
  fun shouldPassNullToBoxedField() {
    val input = bind(BoxedThresholdInput::class.java, listOf(literal("threshold", null)))

    assertNull(input.threshold)
  }

  @Test
  @DisplayName("Should name the field when a primitive one is bound to a null literal")
  fun shouldNameFieldBoundToNullLiteral() {
    val error =
      assertThrows(MissingXComException::class.java) {
        bind(ThresholdInput::class.java, listOf(literal("threshold", null)))
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
        bind(
          ThresholdInput::class.java,
          listOf(mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream")),
        )
      }

    assertEquals(
      "Task parameter 'threshold' requires an XCom from task 'upstream', but none was pushed. " +
        "This parameter has a primitive type that cannot be null; declare it with a boxed type " +
        "(e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }
}
