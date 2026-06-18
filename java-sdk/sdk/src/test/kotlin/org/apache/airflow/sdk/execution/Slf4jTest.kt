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

import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readUTF8Line
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.slf4j.Logger as Slf4jLogger
import org.slf4j.event.Level as Slf4jLevel

class Slf4jTest {
  // Default threshold is INFO, which would suppress the trace/debug cases the
  // mapping and formatting tests exercise; pin TRACE so every level is emitted.
  // Suppression tests override this property explicitly.
  @BeforeEach
  fun emitAllLevels() {
    System.setProperty(LogLevel.LEVEL_PROPERTY, "TRACE")
  }

  @AfterEach
  fun clearLevelProperty() {
    System.clearProperty(LogLevel.LEVEL_PROPERTY)
  }

  private fun captureOrNull(block: (Slf4jLogger) -> Unit): String? {
    val channel = ByteChannel(autoFlush = true)
    LogSender.configure(channel)
    block(AirflowLoggerFactory().getLogger("my.logger"))
    return runBlocking {
      channel.flushAndClose()
      channel.readUTF8Line()
    }
  }

  private fun logAndCapture(block: (Slf4jLogger) -> Unit): JsonObject {
    val line = captureOrNull(block)
    Assertions.assertNotNull(line)
    return Json.parseToJsonElement(line!!).jsonObject
  }

  @Test
  @DisplayName("Should map every SLF4J level to its Airflow wire name")
  fun shouldMapSlf4jLevelsToWireNames() {
    val cases =
      listOf<Pair<(Slf4jLogger) -> Unit, String>>(
        { l: Slf4jLogger -> l.trace("m") } to "debug",
        { l: Slf4jLogger -> l.debug("m") } to "debug",
        { l: Slf4jLogger -> l.info("m") } to "info",
        { l: Slf4jLogger -> l.warn("m") } to "warning",
        { l: Slf4jLogger -> l.error("m") } to "error",
      )
    for ((act, expected) in cases) {
      Assertions.assertEquals(expected, logAndCapture(act)["level"]!!.jsonPrimitive.content)
    }
  }

  @Test
  @DisplayName("Should format placeholders and carry the logger name")
  fun shouldFormatPlaceholdersAndCarryLoggerName() {
    val obj = logAndCapture { it.info("Got XCom from '{}' {}", "extract", 42) }
    Assertions.assertEquals("info", obj["level"]!!.jsonPrimitive.content)
    Assertions.assertEquals("my.logger", obj["logger"]!!.jsonPrimitive.content)
    Assertions.assertEquals("Got XCom from 'extract' 42", obj["event"]!!.jsonPrimitive.content)
  }

  @Test
  @DisplayName("Should attach the stack trace of a logged throwable")
  fun shouldAttachThrowableStackTrace() {
    val obj = logAndCapture { it.error("boom", RuntimeException("kaboom")) }
    Assertions.assertEquals("error", obj["level"]!!.jsonPrimitive.content)
    Assertions.assertTrue(obj["error_detail"]!!.jsonPrimitive.content.contains("kaboom"))
  }

  @Test
  @DisplayName("Should encode a log message into the supervisor's JSON shape")
  fun shouldEncodeLogMessage() {
    val message =
      LogMessage(
        event = "hello",
        arguments = mapOf("k" to "v"),
        loggerName = "the.logger",
        level = Level.WARN,
        timestamp = LocalDateTime(2024, 1, 1, 0, 0, 0),
      )
    val obj = Json.parseToJsonElement(LogSender.encode(message)).jsonObject
    Assertions.assertEquals("warning", obj["level"]!!.jsonPrimitive.content)
    Assertions.assertEquals("the.logger", obj["logger"]!!.jsonPrimitive.content)
    Assertions.assertEquals("hello", obj["event"]!!.jsonPrimitive.content)
    Assertions.assertEquals("v", obj["k"]!!.jsonPrimitive.content)
  }

  @Test
  @DisplayName("Should translate SLF4J levels to Airflow levels")
  fun shouldTranslateSlf4jLevelEnum() {
    Assertions.assertEquals(Level.TRACE, Slf4jLevel.TRACE.toAirflowLevel())
    Assertions.assertEquals(Level.DEBUG, Slf4jLevel.DEBUG.toAirflowLevel())
    Assertions.assertEquals(Level.INFO, Slf4jLevel.INFO.toAirflowLevel())
    Assertions.assertEquals(Level.WARN, Slf4jLevel.WARN.toAirflowLevel())
    Assertions.assertEquals(Level.ERROR, Slf4jLevel.ERROR.toAirflowLevel())
  }

  @Test
  @DisplayName("Should suppress events below the configured threshold")
  fun shouldSuppressBelowThreshold() {
    System.setProperty(LogLevel.LEVEL_PROPERTY, "WARN")
    Assertions.assertNull(captureOrNull { it.trace("nope") })
    Assertions.assertNull(captureOrNull { it.debug("nope") })
    Assertions.assertNull(captureOrNull { it.info("nope") })
    Assertions.assertNotNull(captureOrNull { it.warn("yes") })
    Assertions.assertNotNull(captureOrNull { it.error("yes") })
  }

  @Test
  @DisplayName("Should report enabled levels matching the threshold")
  fun shouldReportEnabledLevelsForThreshold() {
    System.setProperty(LogLevel.LEVEL_PROPERTY, "INFO")
    val logger = AirflowLoggerFactory().getLogger("my.logger")
    Assertions.assertFalse(logger.isTraceEnabled)
    Assertions.assertFalse(logger.isDebugEnabled)
    Assertions.assertTrue(logger.isInfoEnabled)
    Assertions.assertTrue(logger.isWarnEnabled)
    Assertions.assertTrue(logger.isErrorEnabled)
  }

  @Test
  @DisplayName("Should parse level names, aliases, and reject unknown values")
  fun shouldParseLevelNames() {
    Assertions.assertEquals(Level.DEBUG, LogLevel.parse("debug"))
    Assertions.assertEquals(Level.WARN, LogLevel.parse("WARNING"))
    Assertions.assertEquals(Level.WARN, LogLevel.parse(" warn "))
    Assertions.assertEquals(Level.ERROR, LogLevel.parse("CRITICAL"))
    Assertions.assertNull(LogLevel.parse("bogus"))
    Assertions.assertNull(LogLevel.parse(null))
  }

  @Test
  @DisplayName("Should default to INFO when no level is configured")
  fun shouldDefaultToInfoWhenUnconfigured() {
    Assumptions.assumeTrue(System.getenv(LogLevel.LEVEL_ENV) == null)
    System.clearProperty(LogLevel.LEVEL_PROPERTY)
    Assertions.assertEquals(Level.INFO, LogLevel.threshold())
  }
}
