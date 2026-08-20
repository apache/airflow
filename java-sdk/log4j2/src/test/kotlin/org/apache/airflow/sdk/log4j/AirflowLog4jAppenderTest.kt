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

package org.apache.airflow.sdk.log4j

import io.mockk.every
import io.mockk.just
import io.mockk.mockkObject
import io.mockk.runs
import io.mockk.slot
import io.mockk.unmockkAll
import io.mockk.verify
import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import org.apache.logging.log4j.MarkerManager
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.SimpleMessage
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.apache.logging.log4j.Level as L4jLevel

class AirflowLog4jAppenderTest {
  private lateinit var appender: AirflowLog4jAppender

  @BeforeEach
  fun setUp() {
    appender = AirflowLog4jAppender.createAppender("AirflowAppender", null)
    mockkObject(Log)
    every { Log.isEnabledForLevel(any(), any()) } returns true
    every { Log.send(any(), any(), any(), any<Map<String, Any?>>()) } just runs
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `factory honors the configured name and defaults when absent`() {
    assertEquals("Airflow", AirflowLog4jAppender.createAppender("Airflow", null).name)
    assertEquals("AirflowAppender", AirflowLog4jAppender.createAppender(null, null).name)
  }

  @Test
  fun `level conversions`() {
    val cases =
      listOf(
        L4jLevel.FATAL to Level.CRITICAL,
        L4jLevel.ERROR to Level.ERROR,
        L4jLevel.WARN to Level.WARNING,
        L4jLevel.INFO to Level.INFO,
        L4jLevel.DEBUG to Level.DEBUG,
        L4jLevel.TRACE to Level.NOTSET,
        L4jLevel.ALL to Level.NOTSET,
      )
    cases.forEach { (l4jLevel, expected) ->
      val capturedLevel = slot<Level>()
      every { Log.send(capture(capturedLevel), any(), any(), any<Map<String, Any?>>()) } just runs
      appender.append(event("msg", l4jLevel))
      assertEquals(expected, capturedLevel.captured, "Log4j $l4jLevel should map to SDK $expected")
    }
  }

  @Test
  fun `formatted message and logger name are forwarded`() {
    val capturedArgs = slot<Map<String, Any?>>()
    appender.append(event("hello world", L4jLevel.INFO, loggerName = "com.example.Task"))
    verify { Log.send(Level.INFO, "com.example.Task", "hello world", capture(capturedArgs)) }
    assertTrue(capturedArgs.captured.isEmpty())
  }

  @Test
  fun `thrown is stored under the exception key`() {
    val capturedArgs = slot<Map<String, Any?>>()
    every { Log.send(any(), any(), any(), capture(capturedArgs)) } just runs
    appender.append(event("failure", L4jLevel.ERROR, thrown = RuntimeException("kaboom")))
    assertTrue(capturedArgs.captured["exception"].toString().contains("kaboom"))
  }

  @Test
  fun `marker name is stored under the marker key`() {
    val capturedArgs = slot<Map<String, Any?>>()
    every { Log.send(any(), any(), any(), capture(capturedArgs)) } just runs
    appender.append(event("hi", L4jLevel.INFO, marker = MarkerManager.getMarker("AUDIT")))
    assertEquals("AUDIT", capturedArgs.captured["marker"])
  }

  private fun event(
    message: String,
    level: L4jLevel,
    loggerName: String = "test.Logger",
    thrown: Throwable? = null,
    marker: org.apache.logging.log4j.Marker? = null,
  ) = Log4jLogEvent
    .newBuilder()
    .setLoggerName(loggerName)
    .setLevel(level)
    .setMessage(SimpleMessage(message))
    .setThrown(thrown)
    .setMarker(marker)
    .build()
}
