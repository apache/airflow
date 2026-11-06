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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class NamespaceLevelsTest {
  @BeforeEach
  fun setUp() {
    LogCapture.drain() // discard anything buffered by earlier tests
  }

  @Test
  @DisplayName("null, empty and separator-only input parse to no overrides and log nothing")
  fun shouldParseBlankToEmptyMap() {
    for (raw in listOf(null, "", "   ", " , , ")) {
      assertEquals(emptyMap<String, Level>(), NamespaceLevels.parse(raw), "levels for <$raw>")
    }
    assertTrue(LogCapture.drain().isEmpty(), "no log should be emitted for blank input")
  }

  @Test
  @DisplayName("Should parse the documented example without logging")
  fun shouldParseDocumentedExample() {
    assertEquals(
      mapOf("sqlalchemy" to Level.INFO, "sqlalchemy.engine" to Level.DEBUG),
      NamespaceLevels.parse("sqlalchemy=INFO sqlalchemy.engine=DEBUG"),
    )
    assertTrue(LogCapture.drain().isEmpty(), "no log should be emitted for valid input")
  }

  @Test
  @DisplayName("Should accept whitespace, commas and a mix as separators")
  fun shouldAcceptVariousSeparators() {
    val expected = mapOf("a" to Level.INFO, "b" to Level.DEBUG, "c" to Level.ERROR)
    assertEquals(expected, NamespaceLevels.parse("a=INFO b=DEBUG c=ERROR"))
    assertEquals(expected, NamespaceLevels.parse("a=INFO,b=DEBUG,c=ERROR"))
    assertEquals(expected, NamespaceLevels.parse("  a=INFO ,, b=DEBUG\t c=ERROR  "))
  }

  @Test
  @DisplayName("Level names are case-insensitive")
  fun shouldParseLevelCaseInsensitively() {
    assertEquals(
      mapOf("a" to Level.INFO, "b" to Level.CRITICAL),
      NamespaceLevels.parse("a=info b=Critical"),
    )
  }

  @Test
  @DisplayName("When a logger is repeated the last value wins")
  fun shouldLetLastValueWin() {
    assertEquals(mapOf("a" to Level.ERROR), NamespaceLevels.parse("a=INFO a=ERROR"))
  }

  @Test
  @DisplayName("An entry without '=' is skipped, valid entries kept, and an error is logged")
  fun shouldSkipEntryWithoutEquals() {
    // "sqlalchemy" keeps its override; only the malformed "botocore" falls back to the global level.
    assertEquals(mapOf("sqlalchemy" to Level.INFO), NamespaceLevels.parse("sqlalchemy=INFO botocore"))

    val error = LogCapture.drain().single()
    assertEquals(Level.ERROR, error.level)
    assertEquals("org.apache.airflow.sdk.execution.NamespaceLevels", error.logger)
    assertTrue(error.event.contains("AIRFLOW__LOGGING__NAMESPACE_LEVELS"), error.event)
    assertTrue(error.event.contains("botocore"), error.event)
    assertTrue(error.event.contains("<logger>=<level>"), error.event)
  }

  @Test
  @DisplayName("An entry with an empty logger name is skipped while valid entries are kept")
  fun shouldSkipEmptyLoggerName() {
    assertEquals(mapOf("a" to Level.DEBUG), NamespaceLevels.parse("=INFO a=DEBUG"))
    assertTrue(
      LogCapture
        .drain()
        .single()
        .event
        .contains("logger name is empty"),
    )
  }

  @Test
  @DisplayName("An unknown level is skipped and reported with the valid levels")
  fun shouldSkipUnknownLevel() {
    assertEquals(emptyMap<String, Level>(), NamespaceLevels.parse("sqlalchemy=VERBOSE"))

    val event = LogCapture.drain().single().event
    assertTrue(event.contains("VERBOSE"), event)
    assertTrue(event.contains("sqlalchemy"), event)
    assertTrue(event.contains("INFO"), event)
  }

  @Test
  @DisplayName("An entry with an empty level is skipped")
  fun shouldSkipEmptyLevel() {
    assertEquals(emptyMap<String, Level>(), NamespaceLevels.parse("sqlalchemy="))
    assertEquals(1, LogCapture.drain().size)
  }

  @Test
  @DisplayName("Every invalid entry is reported in a single log, valid ones kept")
  fun shouldReportEveryInvalidEntry() {
    assertEquals(
      mapOf("a" to Level.INFO, "c" to Level.DEBUG),
      NamespaceLevels.parse("a=INFO botocore b=NOPE c=DEBUG"),
    )

    val event = LogCapture.drain().single().event
    assertTrue(event.contains("botocore"), event)
    assertTrue(event.contains("NOPE"), event)
  }
}
