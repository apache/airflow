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

package org.apache.airflow.sdk.slf4j

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.LogCapture
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.MarkerFactory
import org.slf4j.event.Level as SLevel

class AirflowSlf4jLoggerTest {
  private lateinit var logger: AirflowSlf4jLogger

  @BeforeEach
  fun setUp() {
    logger = AirflowSlf4jLogger("com.example.Task")
    LogCapture.resetThresholds()
    LogCapture.drain() // discard any messages buffered before this test
  }

  @Test
  fun `level conversions`() {
    val cases =
      listOf(
        SLevel.TRACE to Level.NOTSET,
        SLevel.DEBUG to Level.DEBUG,
        SLevel.INFO to Level.INFO,
        SLevel.WARN to Level.WARNING,
        SLevel.ERROR to Level.ERROR,
      )
    cases.forEach { (slf4jLevel, expected) ->
      LogCapture.drain()
      when (slf4jLevel) {
        SLevel.TRACE -> logger.trace("m")
        SLevel.DEBUG -> logger.debug("m")
        SLevel.INFO -> logger.info("m")
        SLevel.WARN -> logger.warn("m")
        SLevel.ERROR -> logger.error("m")
      }
      val messages = LogCapture.drain().filter { it.logger == "com.example.Task" }
      assertEquals(1, messages.size, "Expected exactly one message for SLF4J $slf4jLevel")
      assertEquals(expected, messages.single().level, "SLF4J $slf4jLevel should map to SDK $expected")
    }
  }

  @Test
  fun `message and logger name are forwarded`() {
    logger.info("hello")
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals(Level.INFO, msg.level)
    assertEquals("com.example.Task", msg.logger)
    assertEquals("hello", msg.event)
  }

  @Test
  fun `message parameters are rendered into the message`() {
    logger.info("{} {}", "alpha", 42 as Any)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("alpha 42", msg.event)
    assertFalse(msg.arguments.containsKey("0"))
  }

  @Test
  fun `throwable is stored under the exception key`() {
    val ex = RuntimeException("boom")
    logger.error("oops", ex)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertTrue(msg.arguments["exception"].toString().contains("boom"))
  }

  @Test
  fun `marker name is stored under the marker key`() {
    logger.info(MarkerFactory.getMarker("AUDIT"), "hello")
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("AUDIT", msg.arguments["marker"])
  }

  @Test
  fun `no marker key is added when none is supplied`() {
    logger.info("hello")
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertFalse(msg.arguments.containsKey("marker"))
  }

  @Test
  fun `named DEBUG override enables debug while the global level stays INFO`() {
    LogCapture.configureThresholds(Level.INFO, mapOf("com.example.Task" to Level.DEBUG))

    // AbstractLogger gates debug() on isDebugEnabled(); the per-logger override must let it through.
    assertTrue(logger.isDebugEnabled)
    logger.debug("hello")

    val messages = LogCapture.drain().filter { it.logger == "com.example.Task" }
    assertEquals(1, messages.size, "named DEBUG override should let debug through")
    assertEquals(Level.DEBUG, messages.single().level)

    // A logger without an override still follows the global INFO threshold.
    assertFalse(AirflowSlf4jLogger("com.other.Task").isDebugEnabled)
  }
}
