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

import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.Date
import java.util.Optional
import java.util.UUID

private fun nullable(schema: Map<String, Any?>) = mapOf("anyOf" to listOf(schema, mapOf("type" to "null")))

private val INT64 = mapOf("type" to "integer", "format" to "int64")
private val STRING = mapOf("type" to "string")

private class Payload

private enum class Color { RED, GREEN }

private object EnumInitialization {
  var ran = false
}

private enum class Lazy {
  ONLY,
  ;

  init {
    EnumInitialization.ran = true
  }
}

/** Declares one field per case, so each keeps the generic type a Kotlin declaration compiles to. */
@Suppress("unused")
private class Declared {
  @JvmField var flag: Boolean = false

  @JvmField var boxedFlag: Boolean? = null

  @JvmField var tiny: Byte = 0

  @JvmField var small: Short = 0

  @JvmField var count: Int = 0

  @JvmField var total: Long = 0

  @JvmField var boxedTotal: Long? = null

  @JvmField var ratio: Float = 0f

  @JvmField var score: Double = 0.0

  @JvmField var letter: Char = 'a'

  @JvmField var text: String? = null

  @JvmField var big: BigInteger? = null

  @JvmField var exact: BigDecimal? = null

  @JvmField var id: UUID? = null

  @JvmField var color: Color? = null

  @JvmField var at: OffsetDateTime? = null

  @JvmField var instant: Instant? = null

  @JvmField var day: LocalDate? = null

  @JvmField var time: LocalTime? = null

  @JvmField var wait: Duration? = null

  @JvmField var names: List<String>? = null

  @JvmField var anything: List<Any>? = null

  @JvmField var words: Array<String>? = null

  @JvmField var totals: LongArray? = null

  @JvmField var bytes: ByteArray? = null

  @JvmField var counts: Map<String, Long>? = null

  @JvmField var payload: Payload? = null

  @JvmField var any: Any? = null

  @JvmField var date: Date? = null

  @JvmField var maybe: Optional<Long>? = null

  // Kotlin compiles a non-final type argument of a parameter to a wildcard, `List<? extends Number>`.
  fun acceptNumbers(numbers: List<Number>) = numbers
}

private fun schemaOf(field: String) = buildValueSchema(Declared::class.java.getField(field).genericType)

class ValueSchemaTest {
  @Test
  @DisplayName("Should describe scalars in pydantic's vocabulary, excluding null only for primitives")
  fun describesScalars() {
    assertAll(
      { assertEquals(mapOf("type" to "boolean"), schemaOf("flag")) },
      { assertEquals(nullable(mapOf("type" to "boolean")), schemaOf("boxedFlag")) },
      { assertEquals(mapOf("type" to "integer", "minimum" to -128L, "maximum" to 127L), schemaOf("tiny")) },
      { assertEquals(mapOf("type" to "integer", "minimum" to -32768L, "maximum" to 32767L), schemaOf("small")) },
      { assertEquals(mapOf("type" to "integer", "format" to "int32"), schemaOf("count")) },
      { assertEquals(INT64, schemaOf("total")) },
      { assertEquals(nullable(INT64), schemaOf("boxedTotal")) },
      { assertEquals(mapOf("type" to "number", "format" to "float"), schemaOf("ratio")) },
      { assertEquals(mapOf("type" to "number", "format" to "double"), schemaOf("score")) },
      { assertEquals(mapOf("type" to "string", "minLength" to 1, "maxLength" to 1), schemaOf("letter")) },
      { assertEquals(nullable(STRING), schemaOf("text")) },
      { assertEquals(nullable(mapOf("type" to "integer")), schemaOf("big")) },
      { assertEquals(nullable(mapOf("type" to "number")), schemaOf("exact")) },
      { assertEquals(nullable(mapOf("type" to "string", "format" to "uuid")), schemaOf("id")) },
      { assertEquals(nullable(mapOf("type" to "string", "enum" to listOf("RED", "GREEN"))), schemaOf("color")) },
    )
  }

  @Test
  @DisplayName("Should describe temporal types by their ISO-8601 string format")
  fun describesTemporalTypes() {
    assertAll(
      { assertEquals(nullable(mapOf("type" to "string", "format" to "date-time")), schemaOf("at")) },
      { assertEquals(nullable(mapOf("type" to "string", "format" to "date-time")), schemaOf("instant")) },
      { assertEquals(nullable(mapOf("type" to "string", "format" to "date")), schemaOf("day")) },
      { assertEquals(nullable(mapOf("type" to "string", "format" to "time")), schemaOf("time")) },
      { assertEquals(nullable(mapOf("type" to "string", "format" to "duration")), schemaOf("wait")) },
    )
  }

  @Test
  @DisplayName("Should describe containers with the schema of what they hold")
  fun describesContainers() {
    assertAll(
      { assertEquals(nullable(mapOf("type" to "array", "items" to nullable(STRING))), schemaOf("names")) },
      { assertEquals(nullable(mapOf("type" to "array", "items" to emptyMap<String, Any?>())), schemaOf("anything")) },
      {
        assertEquals(
          nullable(mapOf("type" to "array", "items" to nullable(mapOf("type" to "number")))),
          buildValueSchema(Declared::class.java.getMethod("acceptNumbers", List::class.java).genericParameterTypes[0]),
        )
      },
      { assertEquals(nullable(mapOf("type" to "array", "items" to nullable(STRING))), schemaOf("words")) },
      { assertEquals(nullable(mapOf("type" to "array", "items" to INT64)), schemaOf("totals")) },
      {
        assertEquals(
          nullable(mapOf("type" to "object", "additionalProperties" to nullable(INT64))),
          schemaOf("counts"),
        )
      },
      {
        assertEquals(
          nullable(mapOf("type" to "array", "items" to emptyMap<String, Any?>())),
          buildValueSchema(java.util.List::class.java),
        )
      },
      {
        assertEquals(
          nullable(mapOf("type" to "object", "additionalProperties" to true)),
          buildValueSchema(java.util.Map::class.java),
        )
      },
      { assertEquals(nullable(mapOf("type" to "object")), schemaOf("payload")) },
    )
  }

  @Test
  @DisplayName("Should say nothing for a type that accepts more than one shape, or none")
  fun describesNothingForOpenTypes() {
    assertAll(
      { assertEquals(null, schemaOf("bytes")) },
      { assertEquals(null, schemaOf("any")) },
      { assertEquals(null, schemaOf("date")) },
      { assertEquals(null, schemaOf("maybe")) },
    )
  }

  @Test
  @DisplayName("Should list an enum's constants without initializing it")
  fun listsEnumConstantsWithoutInitializing() {
    assertEquals(nullable(mapOf("type" to "string", "enum" to listOf("ONLY"))), buildValueSchema(Lazy::class.java))
    assertFalse(EnumInitialization.ran)
  }
}
