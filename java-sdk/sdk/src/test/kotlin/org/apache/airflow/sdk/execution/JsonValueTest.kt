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

package org.apache.airflow.sdk.execution

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import scala.fake.CaseClassLike
import scala.fake.OptionLike
import scala.fake.SeqLike
import java.math.BigDecimal
import java.math.BigInteger
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.Optional

class JsonValueTest {
  @Test
  @DisplayName("jsonValue returns JSON scalars, maps, lists and arrays unchanged")
  fun acceptsJsonTypes() {
    val map = mapOf("region" to "eu", "threshold" to 0.5)
    val list = listOf(1, "x", null)
    Assertions.assertNull(jsonValue(null))
    Assertions.assertEquals("ok", jsonValue("ok"))
    Assertions.assertEquals(true, jsonValue(true))
    Assertions.assertEquals(42, jsonValue(42))
    Assertions.assertEquals(42L, jsonValue(42L))
    Assertions.assertEquals(1.5, jsonValue(1.5))
    Assertions.assertEquals(BigInteger.TEN, jsonValue(BigInteger.TEN))
    Assertions.assertSame(map, jsonValue(map))
    Assertions.assertSame(list, jsonValue(list))
    Assertions.assertSame(emptyMap<String, Any?>(), jsonValue(emptyMap<String, Any?>()))
    val array = arrayOf("a", "b")
    Assertions.assertSame(array, jsonValue(array))
  }

  @Test
  @DisplayName("jsonValue returns Java types that already encoded usefully, unchanged")
  fun acceptsJavaTypesThatJacksonEncodes() {
    val score = Score("eu", 0.5)
    val at = OffsetDateTime.of(2024, 12, 1, 1, 0, 0, 0, ZoneOffset.UTC)
    Assertions.assertSame(score, jsonValue(score))
    Assertions.assertSame(Region.EU, jsonValue(Region.EU))
    Assertions.assertSame(at, jsonValue(at))
    val date = Date(0)
    Assertions.assertSame(date, jsonValue(date))
    Assertions.assertEquals(BigDecimal("1.10"), jsonValue(BigDecimal("1.10")))
    Assertions.assertFalse(Frame.encodesAsEmptyObject(score))
    Assertions.assertFalse(Frame.encodesAsEmptyObject(Region.EU))
    Assertions.assertFalse(Frame.encodesAsEmptyObject(at))
  }

  @Test
  @DisplayName("jsonValue does not reject Optional; encoding is left to Jackson")
  fun doesNotRejectOptional() {
    // Current Jackson needs jackson-datatype-jdk8 to encode Optional. The
    // previous convertValue path failed with that error; this check must not
    // replace it with a different one, or treat Optional as `{}`.
    val present = Optional.of("x")
    val empty = Optional.empty<String>()
    Assertions.assertSame(present, jsonValue(present))
    Assertions.assertSame(empty, jsonValue(empty))
    Assertions.assertFalse(Frame.encodesAsEmptyObject(present))
    Assertions.assertFalse(Frame.encodesAsEmptyObject(empty))
  }

  @Test
  @DisplayName("jsonValue rejects a byte array with the existing Base64 hint")
  fun rejectsByteArray() {
    val error = Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(byteArrayOf(1, 2)) }
    Assertions.assertTrue("Base64" in error.message.orEmpty(), error.message)
  }

  @Test
  @DisplayName("jsonValue rejects a nested byte array")
  fun rejectsNestedByteArray() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        jsonValue(mapOf("blob" to byteArrayOf(1)))
      }
    Assertions.assertTrue("Base64" in error.message.orEmpty(), error.message)
  }

  @Test
  @DisplayName("jsonValue rejects an object with no properties instead of encoding it as {}")
  fun rejectsObjectWithNoProperties() {
    val error = Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(NoProperties()) }
    assertRejected(error, NoProperties::class.java, "would be stored as {}")
    Assertions.assertTrue(Frame.encodesAsEmptyObject(NoProperties()))
  }

  @Test
  @DisplayName("jsonValue rejects a nested object with no properties")
  fun rejectsNestedEmptyBean() {
    val error = Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(listOf(NoProperties())) }
    assertRejected(error, NoProperties::class.java, "would be stored as {}")
  }

  @Test
  @DisplayName("jsonValue rejects stand-in Scala collections, Option and case classes")
  fun rejectsScalaTypes() {
    val seq = Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(SeqLike()) }
    val option = Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(OptionLike()) }
    val caseClass =
      Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(CaseClassLike("eu", 0.5)) }
    val viaInterface =
      Assertions.assertThrows(IllegalArgumentException::class.java) { jsonValue(LooksLikeCaseClass("eu")) }
    assertRejected(seq, SeqLike::class.java, "CollectionConverters")
    assertRejected(option, OptionLike::class.java, "CollectionConverters")
    assertRejected(caseClass, CaseClassLike::class.java, "case class")
    assertRejected(viaInterface, LooksLikeCaseClass::class.java, "CollectionConverters")
    Assertions.assertTrue(isScalaType(SeqLike::class.java))
    Assertions.assertTrue(isScalaType(LooksLikeCaseClass::class.java))
    Assertions.assertFalse(isScalaType(Score::class.java))
  }

  @Test
  @DisplayName("jsonValue rejects a nested Scala type")
  fun rejectsNestedScalaType() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        jsonValue(mapOf("rows" to SeqLike()))
      }
    assertRejected(error, SeqLike::class.java, "CollectionConverters")
  }

  private fun assertRejected(
    error: IllegalArgumentException,
    type: Class<*>,
    expectedSnippet: String,
  ) {
    val message = error.message.orEmpty()
    Assertions.assertTrue(type.name in message, message)
    Assertions.assertTrue(expectedSnippet in message, message)
  }

  /** A class with no JavaBean properties; Jackson would store `{}`. */
  private class NoProperties

  /** A class Jackson stores as its bean properties. */
  @Suppress("unused")
  private class Score(
    val region: String,
    val threshold: Double,
  )

  private enum class Region { EU }

  /** A user type whose interface is under `scala.`, like a case class implementing Product. */
  private class LooksLikeCaseClass(
    val region: String,
  ) : scala.fake.Product
}
