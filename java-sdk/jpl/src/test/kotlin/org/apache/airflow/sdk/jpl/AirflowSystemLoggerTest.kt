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

package org.apache.airflow.sdk.jpl

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.LogCapture
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.ResourceBundle

class AirflowSystemLoggerTest {
  private lateinit var logger: AirflowSystemLogger

  @BeforeEach
  fun setUp() {
    logger = AirflowSystemLogger("com.example.Task")
    LogCapture.resetThresholds()
    LogCapture.drain()
  }

  @Test
  fun `level conversions`() {
    val cases =
      listOf(
        System.Logger.Level.TRACE to Level.NOTSET,
        System.Logger.Level.DEBUG to Level.DEBUG,
        System.Logger.Level.INFO to Level.INFO,
        System.Logger.Level.WARNING to Level.WARNING,
        System.Logger.Level.ERROR to Level.ERROR,
      )
    cases.forEach { (sysLevel, expected) ->
      LogCapture.drain()
      logger.log(sysLevel, null as ResourceBundle?, "m", null as Array<out Any?>?)
      val messages = LogCapture.drain().filter { it.logger == "com.example.Task" }
      assertEquals(1, messages.size, "Expected exactly one message for System.Logger $sysLevel")
      assertEquals(expected, messages.single().level, "System.Logger $sysLevel should map to SDK $expected")
    }
  }

  @Test
  fun `OFF level is not forwarded`() {
    logger.log(System.Logger.Level.OFF, null as ResourceBundle?, "should not appear", null as Array<out Any?>?)
    assertTrue(LogCapture.drain().none { it.logger == "com.example.Task" })
  }

  @Test
  fun `message and logger name are forwarded`() {
    logger.log(System.Logger.Level.INFO, null as ResourceBundle?, "hello", null as Array<out Any?>?)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals(Level.INFO, msg.level)
    assertEquals("com.example.Task", msg.logger)
    assertEquals("hello", msg.event)
  }

  @Test
  fun `null params array is tolerated (matches JDK default method delegation)`() {
    logger.log(System.Logger.Level.INFO, null as ResourceBundle?, "hello", null as Array<out Any?>?)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("hello", msg.event)
    assertEquals(emptyMap<String, Any?>(), msg.arguments)
  }

  @Test
  fun `null throwable is tolerated (matches JDK contract)`() {
    logger.log(System.Logger.Level.INFO, null as ResourceBundle?, "hello", null as Throwable?)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("hello", msg.event)
    assertEquals(emptyMap<String, Any?>(), msg.arguments)
  }

  @Test
  fun `message parameters are rendered into the message`() {
    logger.log(System.Logger.Level.INFO, null as ResourceBundle?, "{0} {1}", arrayOf<Any?>("alpha", 42))
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("alpha 42", msg.event)
    assertEquals(emptyMap<String, Any?>(), msg.arguments)
  }

  @Test
  fun `malformed pattern keeps the template and preserves parameters as metadata`() {
    // "{0" is an unterminated MessageFormat element, so rendering throws and we fall back.
    logger.log(System.Logger.Level.INFO, null as ResourceBundle?, "{0", arrayOf<Any?>("alpha", 42))
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("{0", msg.event)
    assertEquals("alpha", msg.arguments["0"])
    assertEquals(42, msg.arguments["1"])
  }

  @Test
  fun `throwable is stored under the exception key`() {
    val ex = RuntimeException("boom")
    logger.log(System.Logger.Level.ERROR, null as ResourceBundle?, "oops", ex)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertTrue(msg.arguments["exception"].toString().contains("boom"))
  }

  @Test
  fun `bundle key is resolved to localised string`() {
    val bundle =
      object : ResourceBundle() {
        override fun handleGetObject(key: String) = if (key == "greeting") "hello" else null

        override fun getKeys() = java.util.Collections.enumeration(listOf("greeting"))
      }
    logger.log(System.Logger.Level.INFO, bundle, "greeting", null as Array<out Any?>?)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("hello", msg.event)
  }

  @Test
  fun `missing bundle key falls back to the key itself`() {
    val bundle =
      object : ResourceBundle() {
        override fun handleGetObject(key: String) = null

        override fun getKeys() = java.util.Collections.emptyEnumeration<String>()
      }
    logger.log(System.Logger.Level.INFO, bundle, "unknown.key", null as Array<out Any?>?)
    val msg = LogCapture.drain().single { it.logger == "com.example.Task" }
    assertEquals("unknown.key", msg.event)
  }
}
