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
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class LogSenderTest {
  @Test
  @DisplayName("Should make a sender wait while another thread sends a message")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun sendWaitsForAnotherSend() =
    assertSecondSenderWaits { channel, message ->
      LogSender.configure(channel)
      thread { LogSender.send(message) }
    }

  @Test
  @DisplayName("Should make a sender wait while configure writes the messages queued before it")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun sendWaitsForConfigure() =
    assertSecondSenderWaits { channel, message ->
      LogSender.messages.add(message)
      thread { LogSender.configure(channel) }
    }

  /**
   * Checks that a sender waits while another thread writes to [LogSender]'s channel.
   *
   * [startFirst] starts the thread that writes first. That thread gets a message larger than the
   * 1 MiB that a ByteChannel buffers, so the thread stops in the middle of the write until the test
   * reads the channel.
   */
  private fun assertSecondSenderWaits(startFirst: (channel: ByteChannel, message: LogMessage) -> Thread) {
    val channel = ByteChannel(autoFlush = true)
    LogSender.messages.clear()
    try {
      val large = "x".repeat(2 * 1024 * 1024)
      val first = startFirst(channel, LogMessage(large, emptyMap(), "test", Level.INFO))
      val parked = setOf(Thread.State.WAITING, Thread.State.TIMED_WAITING)
      while (first.state !in parked || first.stackTrace.none { it.methodName == "sendTo" }) {
        Assertions.assertNotEquals(Thread.State.TERMINATED, first.state, "the first message no longer fills the channel")
        Thread.sleep(10)
      }
      val second = thread { LogSender.send(LogMessage("small", emptyMap(), "test", Level.INFO)) }
      while (second.state !in parked + Thread.State.BLOCKED + Thread.State.TERMINATED) Thread.sleep(10)

      Assertions.assertEquals(Thread.State.BLOCKED, second.state, "the second sender should wait for the first writer")

      val events =
        runBlocking {
          List(2) {
            Json
              .parseToJsonElement(channel.readLine()!!)
              .jsonObject["event"]!!
              .jsonPrimitive.content
          }
        }
      first.join()
      second.join()
      Assertions.assertEquals(listOf(large.length, 5), events.map { it.length }, "both messages should arrive whole, in order")
    } finally {
      // Once its channel is closed, LogSender queues messages again. Later tests rely on this queuing.
      channel.cancel(null)
    }
  }
}
