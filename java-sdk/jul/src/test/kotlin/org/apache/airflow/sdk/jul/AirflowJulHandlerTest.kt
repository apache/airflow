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

package org.apache.airflow.sdk.jul

import io.mockk.every
import io.mockk.just
import io.mockk.mockkObject
import io.mockk.runs
import io.mockk.slot
import io.mockk.unmockkAll
import io.mockk.verify
import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.logging.ConsoleHandler
import java.util.logging.LogRecord
import java.util.logging.Logger
import java.util.logging.Level as JLevel

class AirflowJulHandlerTest {
  private lateinit var handler: AirflowJulHandler

  @BeforeEach
  fun setUp() {
    handler = AirflowJulHandler()
    mockkObject(Log)
    every { Log.isEnabledForLevel(any(), any()) } returns true
    every { Log.send(any(), any(), any(), any<MutableMap<String, Any?>.() -> Unit>()) } just runs
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
    // Remove any handlers installed by setup() tests so they don't leak between tests.
    val root = Logger.getLogger("")
    root.handlers.filterIsInstance<AirflowJulHandler>().forEach { root.removeHandler(it) }
  }

  @Test
  fun `setup replaces root handlers with a single AirflowJulHandler`() {
    val root = Logger.getLogger("")
    val original = root.handlers.toList()
    val preexisting = ConsoleHandler()
    try {
      original.forEach { root.removeHandler(it) }
      root.addHandler(preexisting)

      AirflowJulHandler.setup()

      assertEquals(1, root.handlers.size)
      assertTrue(root.handlers[0] is AirflowJulHandler)
      assertFalse(root.handlers.contains(preexisting))

      // Idempotent: a second call still leaves exactly one AirflowJulHandler.
      AirflowJulHandler.setup()
      assertEquals(1, root.handlers.size)
      assertTrue(root.handlers[0] is AirflowJulHandler)
    } finally {
      root.handlers.forEach { root.removeHandler(it) }
      original.forEach { root.addHandler(it) }
    }
  }

  @Test
  fun `setup with clean false keeps existing root handlers`() {
    val root = Logger.getLogger("")
    val original = root.handlers.toList()
    val preexisting = ConsoleHandler()
    try {
      original.forEach { root.removeHandler(it) }
      root.addHandler(preexisting)

      AirflowJulHandler.setup(clean = false)

      assertEquals(2, root.handlers.size)
      assertTrue(root.handlers.contains(preexisting))
      assertEquals(1, root.handlers.count { it is AirflowJulHandler })

      // Idempotent: a second call does not add another AirflowJulHandler.
      AirflowJulHandler.setup(clean = false)
      assertEquals(2, root.handlers.size)
      assertEquals(1, root.handlers.count { it is AirflowJulHandler })
    } finally {
      root.handlers.forEach { root.removeHandler(it) }
      original.forEach { root.addHandler(it) }
    }
  }

  // Mapping:
  //   > 1000 -> CRITICAL
  //   > 900  -> ERROR   (SEVERE = 1000)
  //   > 800  -> WARNING (WARNING = 900)
  //   >= 700 -> INFO    (INFO = 800, CONFIG = 700)
  //   >= 400 -> DEBUG   (FINE = 500, FINER = 400)
  //   else   -> NOTSET  (FINEST = 300, ALL)
  @Test
  fun `level conversions`() {
    // Custom level above SEVERE to hit the CRITICAL branch.
    val aboveSevere = object : JLevel("ABOVE_SEVERE", 1001) {}

    val cases =
      listOf(
        aboveSevere to Level.CRITICAL,
        JLevel.SEVERE to Level.ERROR,
        JLevel.WARNING to Level.WARNING,
        JLevel.INFO to Level.INFO,
        JLevel.CONFIG to Level.INFO,
        JLevel.FINE to Level.DEBUG,
        JLevel.FINER to Level.DEBUG,
        JLevel.FINEST to Level.NOTSET,
      )
    cases.forEach { (julLevel, expected) ->
      val capturedLevel = slot<Level>()
      every { Log.send(capture(capturedLevel), any(), any(), any<MutableMap<String, Any?>.() -> Unit>()) } just runs
      handler.publish(record("msg", julLevel))
      assertEquals(expected, capturedLevel.captured, "JUL $julLevel (${julLevel.intValue()}) should map to SDK $expected")
    }
  }

  @Test
  fun `message and logger name are forwarded`() {
    handler.publish(record("hello world", JLevel.INFO, loggerName = "com.example.Task"))
    verify { Log.send(Level.INFO, "com.example.Task", "hello world", any<MutableMap<String, Any?>.() -> Unit>()) }
  }

  @Test
  fun `message parameters are rendered into the message`() {
    val rec =
      record("msg {0} {1}", JLevel.INFO).also {
        it.parameters = arrayOf<Any>("alpha", 42)
      }
    handler.publish(rec)
    verify { Log.send(Level.INFO, "test.Logger", "msg alpha 42", any<MutableMap<String, Any?>.() -> Unit>()) }
  }

  @Test
  fun `thrown is stored under the exception key`() {
    val lambdaSlot = slot<MutableMap<String, Any?>.() -> Unit>()
    every { Log.send(any(), any(), any(), capture(lambdaSlot)) } just runs
    val rec =
      LogRecord(JLevel.SEVERE, "failure").also {
        it.thrown = RuntimeException("kaboom")
      }
    handler.publish(rec)
    val args = mutableMapOf<String, Any?>().also { lambdaSlot.captured.invoke(it) }
    assertTrue(args["exception"].toString().contains("kaboom"))
  }

  private fun record(
    message: String,
    level: JLevel,
    loggerName: String = "test.Logger",
  ) = LogRecord(level, message).also { it.loggerName = loggerName }
}
