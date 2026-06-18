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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.event.Level as SLevel

class AirflowSlf4jLoggerTest {
  private lateinit var logger: AirflowSlf4jLogger

  @BeforeEach
  fun setUp() {
    logger = AirflowSlf4jLogger("com.example.Task")
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
      val messages = LogCapture.drain().filter { it.loggerName == "com.example.Task" }
      assertEquals(1, messages.size, "Expected exactly one message for SLF4J $slf4jLevel")
      assertEquals(expected, messages.single().level, "SLF4J $slf4jLevel should map to SDK $expected")
    }
  }

  @Test
  fun `message and logger name are forwarded`() {
    logger.info("hello")
    val msg = LogCapture.drain().single { it.loggerName == "com.example.Task" }
    assertEquals(Level.INFO, msg.level)
    assertEquals("com.example.Task", msg.loggerName)
    assertEquals("hello", msg.event)
  }

  @Test
  fun `arguments are added to the map indexed by position`() {
    logger.info("{} {}", "alpha", 42 as Any)
    val msg = LogCapture.drain().single { it.loggerName == "com.example.Task" }
    assertEquals("alpha", msg.arguments["0"])
    assertEquals(42, msg.arguments["1"])
  }

  @Test
  fun `throwable is stored under the exception key`() {
    val ex = RuntimeException("boom")
    logger.error("oops", ex)
    val msg = LogCapture.drain().single { it.loggerName == "com.example.Task" }
    assertTrue(msg.arguments["exception"].toString().contains("boom"))
  }
}
