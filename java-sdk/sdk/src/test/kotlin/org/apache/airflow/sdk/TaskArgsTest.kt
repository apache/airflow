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

import org.apache.airflow.sdk.internal.TaskArgs
import org.apache.airflow.sdk.internal.TypeRef
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/** Fixes the type argument one level up, so the anonymous subclass carries none. */
private abstract class NamesRef : TypeRef<List<String>>()

private fun argsWith(
  bindings: List<Map<String, Any?>>?,
  xcoms: Map<String, Any?> = emptyMap(),
  declared: Int = bindings?.size ?: 0,
): Pair<TaskArgs, FakeXComTransport> {
  val (client, transport) = clientWith(bindings, xcoms)
  return TaskArgs.of(taskContext(), client, declared) to transport
}

internal class TaskArgsTest {
  @Test
  @DisplayName("Should resolve a literal binding to its inline value without reading an XCom")
  fun shouldResolveLiteralBinding() {
    val (args, transport) = argsWith(listOf(mapOf("kind" to "literal", "name" to "x", "value" to 42L)))

    assertEquals(42L, args.get(0, java.lang.Long::class.java))
    assertEquals(emptyList<Pair<String, Int?>>(), transport.pulls)
  }

  @Test
  @DisplayName("Should resolve an xcom binding by pulling the bound task's return value")
  fun shouldResolveXComBinding() {
    val (args, transport) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "map_index" to -1L)),
        xcoms = mapOf("upstream" to 7L),
      )

    assertEquals(7L, args.get(0, java.lang.Long::class.java))
    assertEquals(listOf("upstream" to null), transport.pulls)
  }

  @Test
  @DisplayName("Should keep bindings in stub-signature order")
  fun shouldKeepBindingOrder() {
    val (args, _) =
      argsWith(
        listOf(
          mapOf("kind" to "literal", "name" to "b", "value" to 2L),
          mapOf("kind" to "literal", "name" to "a", "value" to 1L),
        ),
      )

    assertEquals(2L, args.get(0, java.lang.Long::class.java))
    assertEquals(1L, args.get(1, java.lang.Long::class.java))
  }

  @Test
  @DisplayName("Should open a task declaring no data parameters when the supervisor sent no bindings")
  fun shouldAcceptNoArguments() {
    assertDoesNotThrow { argsWith(null) }
  }

  @Test
  @DisplayName("Should pass a non-negative bound map index to the XCom read")
  fun shouldPassBoundMapIndex() {
    val (args, transport) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "map_index" to 2L)),
        xcoms = mapOf("upstream" to 7L),
      )

    args.get(0, java.lang.Long::class.java)

    assertEquals(listOf("upstream" to 2), transport.pulls)
  }

  @Test
  @DisplayName("Should index into a list XCom when the binding has an element index")
  fun shouldResolveElementIndex() {
    val (args, _) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
        xcoms = mapOf("upstream" to listOf("a", "b", "c")),
      )

    assertEquals("b", args.get(0, String::class.java))
  }

  @Test
  @DisplayName("Should fail when an element index points into a non-list XCom")
  fun shouldRejectElementIndexOnNonList() {
    val (args, _) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
        xcoms = mapOf("upstream" to "scalar"),
      )

    assertThrows(IllegalStateException::class.java) { args.get(0, String::class.java) }
  }

  @Test
  @DisplayName("Should fail when an element index points past the end of a list XCom")
  fun shouldRejectElementIndexOutOfBounds() {
    val (args, _) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 3L)),
        xcoms = mapOf("upstream" to listOf("a", "b")),
      )

    val error = assertThrows(IllegalStateException::class.java) { args.get(0, String::class.java) }

    assertEquals(
      "Argument 'x' binds element 3 of task 'upstream', but its XCom holds only 2 element(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should pass null through an element index when the upstream pushed nothing")
  fun shouldPassNullThroughElementIndex() {
    val (args, _) =
      argsWith(
        listOf(mapOf("kind" to "xcom", "name" to "x", "task_id" to "upstream", "element_index" to 1L)),
      )

    assertNull(args.get(0, String::class.java))
  }

  @Test
  @DisplayName("Should fail on an unsupported binding kind")
  fun shouldRejectUnknownBindingKind() {
    assertThrows(IllegalStateException::class.java) {
      argsWith(listOf(mapOf("kind" to "mystery", "name" to "x")))
    }
  }

  @Test
  @DisplayName("Should fail on duplicate binding names")
  fun shouldRejectDuplicateBindingNames() {
    assertThrows(IllegalStateException::class.java) {
      argsWith(
        listOf(
          mapOf("kind" to "literal", "name" to "x", "value" to 1L),
          mapOf("kind" to "literal", "name" to "x", "value" to 2L),
        ),
      )
    }
  }

  @Test
  @DisplayName("Should widen a wire integer into the declared numeric type")
  fun shouldWidenNumericBinding() {
    val (args, _) = argsWith(listOf(mapOf("kind" to "literal", "name" to "x", "value" to 5L)))

    assertEquals(5, args.require(0, Integer::class.java).toInt())
  }

  @Test
  @DisplayName("Should keep the element type of a generic parameter read through TypeRef")
  fun shouldKeepGenericElementType() {
    val (args, _) =
      argsWith(listOf(mapOf("kind" to "literal", "name" to "values", "value" to listOf(1L, 2L))))

    val values = args.require(0, object : TypeRef<List<Double>>() {})

    assertEquals(listOf(1.0, 2.0), values)
  }

  @Test
  @DisplayName("Should pass null through get for both the plain and the generic read")
  fun shouldPassNullThrough() {
    val (args, _) =
      argsWith(
        listOf(
          mapOf("kind" to "literal", "name" to "scalar", "value" to null),
          mapOf("kind" to "literal", "name" to "values", "value" to null),
        ),
      )

    assertNull(args.get(0, String::class.java))
    assertNull(args.get(1, object : TypeRef<List<String>>() {}))
  }

  @Test
  @DisplayName("Should fail fast when the stub call bound fewer arguments than the task declares")
  fun shouldFailWhenFewerArgumentsBoundThanDeclared() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        argsWith(listOf(mapOf("kind" to "literal", "name" to "only", "value" to 1L)), declared = 2)
      }

    assertEquals(
      "Task 't' declares 2 data parameter(s) but the stub call bound 1 argument(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should drop a captured default the method does not declare")
  fun shouldDropCapturedDefaultTheMethodOmits() {
    val (args, _) =
      argsWith(
        listOf(
          mapOf("kind" to "literal", "name" to "rows", "value" to 1L),
          mapOf("kind" to "literal", "name" to "note", "value" to "unset", "from_default" to true),
        ),
        declared = 1,
      )

    assertEquals(1L, args.get(0, java.lang.Long::class.java))
  }

  @Test
  @DisplayName("Should keep a captured default the method does declare")
  fun shouldKeepCapturedDefaultTheMethodDeclares() {
    val (args, _) =
      argsWith(
        listOf(
          mapOf("kind" to "literal", "name" to "rows", "value" to 1L),
          mapOf("kind" to "literal", "name" to "note", "value" to "unset", "from_default" to true),
        ),
        declared = 2,
      )

    assertEquals("unset", args.get(1, String::class.java))
  }

  @Test
  @DisplayName("Should report both counts when dropping captured defaults still leaves a mismatch")
  fun shouldFailWhenDroppingDefaultsStillMismatches() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        argsWith(
          listOf(
            mapOf("kind" to "literal", "name" to "rows", "value" to 1L),
            mapOf("kind" to "literal", "name" to "ratio", "value" to 2.5),
            mapOf("kind" to "literal", "name" to "note", "value" to "unset", "from_default" to true),
          ),
          declared = 1,
        )
      }

    assertEquals(
      "Task 't' declares 1 data parameter(s) but the stub call bound 3 argument(s); " +
        "2 remain after dropping captured defaults",
      error.message,
    )
  }

  @Test
  @DisplayName("Should fail fast when the stub call bound more arguments than the task declares")
  fun shouldFailWhenMoreArgumentsBoundThanDeclared() {
    val error =
      assertThrows(IllegalStateException::class.java) {
        argsWith(
          listOf(
            mapOf("kind" to "literal", "name" to "kept", "value" to 1L),
            mapOf("kind" to "literal", "name" to "dropped", "value" to 2L),
          ),
          declared = 1,
        )
      }

    assertEquals(
      "Task 't' declares 1 data parameter(s) but the stub call bound 2 argument(s)",
      error.message,
    )
  }

  @Test
  @DisplayName("Should name the stub argument when a required position is bound to a null literal")
  fun shouldNameArgumentBoundToNullLiteral() {
    val (args, _) = argsWith(listOf(mapOf("kind" to "literal", "name" to "region_code", "value" to null)))

    val error = assertThrows(MissingXComException::class.java) { args.require(0, String::class.java) }

    assertEquals(
      "Task parameter 'region_code' of task 't' is bound to a null literal, but has a primitive type " +
        "that cannot be null; declare a boxed type (e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should name the upstream when a required position's XCom was never pushed")
  fun shouldNameUpstreamWhenXComMissing() {
    val (args, _) =
      argsWith(listOf(mapOf("kind" to "xcom", "name" to "threshold", "task_id" to "upstream")))

    val error = assertThrows(MissingXComException::class.java) { args.require(0, Integer::class.java) }

    assertEquals(
      "Task parameter 'threshold' requires an XCom from task 'upstream', but none was pushed. " +
        "This parameter has a primitive type that cannot be null; declare it with a boxed type " +
        "(e.g. Integer instead of int) to receive null.",
      error.message,
    )
  }

  @Test
  @DisplayName("Should reject a TypeRef whose type argument is fixed by an intermediate class")
  fun shouldRejectIndirectTypeRef() {
    val error = assertThrows(IllegalArgumentException::class.java) { object : NamesRef() {} }

    assertEquals(
      "TypeRef needs a concrete type argument on an anonymous subclass, e.g. new TypeRef<List<String>>() {}",
      error.message,
    )
  }
}
