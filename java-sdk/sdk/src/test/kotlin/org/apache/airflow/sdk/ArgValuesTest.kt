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

internal class ArgValuesTest {
  @Test
  @DisplayName("Should resolve a literal binding to its inline value without reading an XCom")
  fun shouldResolveLiteralBinding() {
    val (client, transport) = clientWith(listOf(mapOf("kind" to "literal", "name" to "x", "value" to 42L)))

    assertEquals(42L, ArgValues.optionalInput(taskContext(), client, 0, java.lang.Long::class.java))
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

    assertEquals(7L, ArgValues.optionalInput(taskContext(), client, 0, java.lang.Long::class.java))
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

    assertEquals(2L, ArgValues.optionalInput(taskContext(), client, 0, java.lang.Long::class.java))
    assertEquals(1L, ArgValues.optionalInput(taskContext(), client, 1, java.lang.Long::class.java))
  }

  @Test
  @DisplayName("Should pass a non-negative bound map index to the XCom read")
  fun shouldPassBoundMapIndex() {
    val (client, transport) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "map_index" to 2L)),
        xcoms = mapOf("upstream" to 7L),
      )

    ArgValues.optionalInput(taskContext(), client, 0, java.lang.Long::class.java)

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

    assertEquals("b", ArgValues.optionalInput(taskContext(), client, 0, String::class.java))
  }

  @Test
  @DisplayName("Should fail when an element index points into a non-list XCom")
  fun shouldRejectElementIndexOnNonList() {
    val (client, _) =
      clientWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
        xcoms = mapOf("upstream" to "scalar"),
      )

    assertThrows(IllegalStateException::class.java) {
      ArgValues.optionalInput(taskContext(), client, 0, String::class.java)
    }
  }

  @Test
  @DisplayName("Should fail on an unsupported binding kind")
  fun shouldRejectUnknownBindingKind() {
    val (client, _) = clientWith(listOf(mapOf("kind" to "mystery", "name" to "x")))

    assertThrows(IllegalStateException::class.java) {
      ArgValues.optionalInput(taskContext(), client, 0, String::class.java)
    }
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

    assertThrows(IllegalStateException::class.java) {
      ArgValues.optionalInput(taskContext(), client, 0, String::class.java)
    }
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
