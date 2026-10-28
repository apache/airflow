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
import io.ktor.utils.io.readLine
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Instant
import java.util.TimeZone
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class LogTest {
  @AfterEach
  fun tearDown() {
    LogCapture.resetThresholds()
  }

  @Test
  @DisplayName("Without overrides, a logger is gated by the global threshold")
  fun shouldUseGlobalThreshold() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = emptyMap()

    assertTrue(Log.isEnabledForLevel(Level.WARNING, "any"))
    assertTrue(Log.isEnabledForLevel(Level.INFO, "any"))
    assertFalse(Log.isEnabledForLevel(Level.DEBUG, "any"))
  }

  @Test
  @DisplayName("An exact namespace override applies to its logger; unrelated loggers use global")
  fun shouldApplyNamespaceOverride() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("chatty" to Level.DEBUG)

    assertTrue(Log.isEnabledForLevel(Level.DEBUG, "chatty"))
    assertFalse(Log.isEnabledForLevel(Level.DEBUG, "other"))
    assertTrue(Log.isEnabledForLevel(Level.INFO, "other"))
  }

  @Test
  @DisplayName("A nested logger inherits the level of its nearest configured ancestor")
  fun shouldInheritFromAncestor() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING)

    // foo.bar and foo.bar.rex have no exact entry, so they inherit foo=WARNING.
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.bar"))
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.bar.rex"))
    assertTrue(Log.isEnabledForLevel(Level.WARNING, "foo.bar.rex"))
  }

  @Test
  @DisplayName("The most specific configured ancestor wins")
  fun shouldPreferMostSpecificAncestor() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING, "foo.bar" to Level.DEBUG)

    // foo.bar.rex's nearest ancestor is foo.bar=DEBUG, not foo=WARNING.
    assertTrue(Log.isEnabledForLevel(Level.DEBUG, "foo.bar.rex"))
    // A sibling under foo (but not under foo.bar) still resolves to foo=WARNING.
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.baz"))
  }

  @Test
  @DisplayName("Inheritance respects dotted-segment boundaries, not raw string prefixes")
  fun shouldNotMatchPartialSegmentPrefixes() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING)

    // "foobar" is not a child of "foo"; it must fall back to the global level.
    assertTrue(Log.isEnabledForLevel(Level.INFO, "foobar"))
  }

  @Test
  @DisplayName("Should send the timestamp in UTC when the JVM runs in a non-UTC time zone")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun shouldSendTimestampInUtc() {
    val defaultZone = TimeZone.getDefault()
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Taipei"))
      val before = Instant.now()
      val message = LogMessage("hello", emptyMap(), "test", Level.INFO)
      val after = Instant.now()

      val timestamp = sendAndRead(message)["timestamp"]!!.jsonPrimitive.content
      assertTrue(timestamp.endsWith("Z"), "timestamp should be in UTC with a Z suffix: $timestamp")
      // The extra second on each side allows for the wall clock being adjusted during the test. A local
      // time sent as UTC would still be hours off.
      assertTrue(
        Instant.parse(timestamp) in before.minusSeconds(1)..after.plusSeconds(1),
        "timestamp should be when the message was created: $timestamp",
      )
    } finally {
      TimeZone.setDefault(defaultZone)
    }
  }

  @Test
  @DisplayName("Should keep the seconds of a timestamp that falls on a whole minute")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun shouldKeepSecondsOnWholeMinute() {
    val message = LogMessage("hello", emptyMap(), "test", Level.INFO, Instant.parse("2026-10-13T09:41:00Z"))

    // The supervisor cannot parse a timestamp without seconds, such as 2026-10-13T09:41.
    assertEquals("2026-10-13T09:41:00Z", sendAndRead(message)["timestamp"]!!.jsonPrimitive.content)
  }

  @Test
  @DisplayName("Should not strand a log buffered while the log socket is configured")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun shouldFlushLogBufferedWhileConfigured() {
    val channel = ByteChannel(autoFlush = true)
    val message = LogMessage("started", emptyMap(), "test", Level.INFO)
    val sawNoWriter = CountDownLatch(1)
    val allowBuffering = CountDownLatch(1)
    val configureStarted = CountDownLatch(1)
    try {
      LogSender.reset()
      LogSender.beforeEnqueueWhenNoWriter = {
        sawNoWriter.countDown()
        allowBuffering.await()
      }
      val sender = Thread { LogSender.send(message) }
      sender.start()
      assertTrue(sawNoWriter.await(5, TimeUnit.SECONDS))

      val configurator =
        Thread {
          configureStarted.countDown()
          LogSender.configure(channel)
        }
      configurator.start()
      assertTrue(configureStarted.await(5, TimeUnit.SECONDS))
      allowBuffering.countDown()
      sender.join(5_000)
      configurator.join(5_000)
      assertFalse(sender.isAlive)
      assertFalse(configurator.isAlive)

      val line = runBlocking { Json.parseToJsonElement(channel.readLine()!!) }.jsonObject
      assertEquals(message.event, line["event"]!!.jsonPrimitive.content)
    } finally {
      allowBuffering.countDown()
      LogSender.reset()
    }
  }

  /** Sends [message] through [LogSender] and returns the JSON object that [LogSender] writes for the message. */
  private fun sendAndRead(message: LogMessage): JsonObject {
    val channel = ByteChannel(autoFlush = true)
    try {
      LogSender.reset()
      LogSender.configure(channel)
      LogSender.send(message)
      val line = runBlocking { Json.parseToJsonElement(channel.readLine()!!) }.jsonObject
      assertEquals(message.event, line["event"]!!.jsonPrimitive.content, "the first line should be the message just sent")
      return line
    } finally {
      // Other test classes read LogSender.messages, which LogSender fills only while it has no channel.
      LogSender.reset()
    }
  }
}
