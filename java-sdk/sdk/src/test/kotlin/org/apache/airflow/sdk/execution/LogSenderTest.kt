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
import io.ktor.utils.io.ClosedByteChannelException
import io.ktor.utils.io.readLine
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.IOException
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

  @Test
  @DisplayName("Should throw the error that the write fails with")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun sendThrowsTheWriteError() {
    val channel = ByteChannel(autoFlush = true)
    LogSender.messages.clear()
    try {
      LogSender.configure(channel)
      val closeCause = IOException("closed by the test")
      var thrown: Throwable? = null
      // The message does not fit in the channel's buffer, so send cannot return before the test closes
      // the channel.
      val large = LogMessage("x".repeat(2 * 1024 * 1024), emptyMap(), "test", Level.INFO)
      val sender = thread { thrown = runCatching { LogSender.send(large) }.exceptionOrNull() }
      val parked = setOf(Thread.State.WAITING, Thread.State.TIMED_WAITING)
      while (sender.state !in parked || sender.stackTrace.none { it.methodName == "sendTo" }) {
        Assertions.assertNotEquals(Thread.State.TERMINATED, sender.state, "send should not return before the test closes the channel")
        Thread.sleep(10)
      }
      channel.cancel(closeCause)
      sender.join()

      Assertions.assertInstanceOf(ClosedByteChannelException::class.java, thrown, "send should throw the error of the write itself")
      val causes = generateSequence(thrown) { it.cause }.toList()
      Assertions.assertTrue(closeCause in causes, "the error should come from closing the channel")
    } finally {
      channel.cancel(null)
    }
  }

  @Test
  @DisplayName("Should write a message from an interrupted thread and leave the thread interrupted")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun sendFromInterruptedThread() {
    val channel = ByteChannel(autoFlush = true)
    LogSender.messages.clear()
    try {
      LogSender.configure(channel)
      var thrown: Throwable? = null
      var stillInterrupted = false
      thread {
        // Code that catches an InterruptedException often restores the interrupt status and then logs.
        Thread.currentThread().interrupt()
        thrown = runCatching { LogSender.send(LogMessage("after interrupt", emptyMap(), "test", Level.INFO)) }.exceptionOrNull()
        stillInterrupted = Thread.currentThread().isInterrupted
      }.join()

      Assertions.assertNull(thrown, "send should not throw for a thread that is interrupted")
      Assertions.assertTrue(stillInterrupted, "send should keep the interrupt status of the thread")
      val event =
        runBlocking {
          Json
            .parseToJsonElement(channel.readLine()!!)
            .jsonObject["event"]!!
            .jsonPrimitive.content
        }
      Assertions.assertEquals("after interrupt", event, "the message should be written")
    } finally {
      channel.cancel(null)
    }
  }

  @Test
  @DisplayName("Should not run coroutines that are ready on the calling thread while configure writes queued messages")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun configureRunsNoOtherCoroutine() =
    assertWriteRunsNoOtherCoroutine(configured = false) { channel ->
      LogSender.messages.add(LogMessage("queued", emptyMap(), "test", Level.INFO))
      LogSender.configure(channel)
    }

  @Test
  @DisplayName("Should not run coroutines that are ready on the calling thread while send writes a message")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun sendRunsNoOtherCoroutine() =
    assertWriteRunsNoOtherCoroutine(configured = true) {
      LogSender.send(LogMessage("sent", emptyMap(), "test", Level.INFO))
    }

  /**
   * Checks that [write] does not run other coroutines that are ready on the calling thread.
   *
   * On the thread that serves the task, the task runs in such a coroutine. If [write] ran the task,
   * the task would run while [write] held the [LogSender] lock. The task would then hang as soon as it
   * waited for another thread that logs, such as the thread that reads replies from the supervisor.
   * In this check, the ready coroutine plays the part of the task and waits for a thread that logs.
   */
  private fun assertWriteRunsNoOtherCoroutine(
    configured: Boolean,
    write: (channel: ByteChannel) -> Unit,
  ) {
    val channel = ByteChannel(autoFlush = true)
    LogSender.messages.clear()
    var other: Thread? = null
    try {
      if (configured) LogSender.configure(channel)
      val otherThreadLogged =
        runBlocking {
          val waiting =
            async {
              val logging = thread { LogSender.send(LogMessage("from another thread", emptyMap(), "test", Level.INFO)) }
              other = logging
              logging.join(TimeUnit.SECONDS.toMillis(10))
              !logging.isAlive
            }
          write(channel)
          waiting.await()
        }
      Assertions.assertTrue(otherThreadLogged, "another thread should finish logging while a coroutine waits for that thread")
    } finally {
      other?.join()
      channel.cancel(null)
    }
  }

  /**
   * Checks that a sender waits while another thread writes to [LogSender]'s channel.
   *
   * [startFirst] starts the thread that writes first. That thread writes a message larger than the
   * 1 MiB buffer of a ByteChannel, so its call does not return until the test reads the channel.
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
