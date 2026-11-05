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
import io.ktor.utils.io.availableForRead
import io.ktor.utils.io.readLine
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.io.PrintStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * Each test runs on a separate thread from its timeout, and [cancelChannel] runs after every test. JUnit's
 * default timeout interrupts the test's own thread instead, and the waits in [LogSender] do not stop when
 * the waiting thread is interrupted. So if send waited for each line to be written, a test that sends
 * [largeMessage] would hang the build. With the separate-thread timeout and [cancelChannel], such a test
 * fails at its timeout, and cancelling the test's channel frees the sender thread for the next test.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LogSenderTest {
  private val channel = ByteChannel(autoFlush = true)

  @AfterEach
  fun cancelChannel() {
    // Once its channel is closed, LogSender queues messages again. Later tests rely on this queuing.
    channel.cancel(null)
  }

  @Test
  @DisplayName("Should return from send before the line is written")
  fun sendReturnsBeforeLineIsWritten() =
    withChannel { channel ->
      LogSender.send(largeMessage())

      Assertions.assertEquals(
        LARGE_EVENT,
        readLines(channel, 1).single().event(),
        "the line should be written once the test reads the channel",
      )
    }

  @Test
  @DisplayName("Should write the messages queued before configure first, then later messages in the order they were sent")
  fun writesMessagesInOrder() =
    withChannel(queued = listOf(message("queued"))) { channel ->
      LogSender.send(message("first"))
      LogSender.send(message("second"))

      Assertions.assertEquals(listOf("queued", "first", "second"), readLines(channel, 3).map { it.event() })
    }

  @Test
  @DisplayName("Should print a failed write's error to stderr instead of throwing it to the caller")
  fun reportsFailedWriteOnStderr() {
    val stderr = ByteArrayOutputStream()
    val originalStderr = System.err
    System.setErr(PrintStream(stderr, true))
    try {
      withChannel { channel ->
        LogSender.send(largeMessage())
        channel.cancel(IOException("closed by the test"))
        LogSender.flush()
      }
    } finally {
      System.setErr(originalStderr)
    }

    Assertions.assertTrue(stderr.toString().contains("Cannot send a log line to the supervisor: ")) { "stderr: $stderr" }
  }

  @Test
  @DisplayName("Should write a message from an interrupted thread and leave the thread interrupted")
  fun sendFromInterruptedThread() =
    withChannel { channel ->
      var thrown: Throwable? = null
      var stillInterrupted = false
      thread {
        // Code that catches an InterruptedException often restores the interrupt status and then logs.
        Thread.currentThread().interrupt()
        thrown = runCatching { LogSender.send(message("after interrupt")) }.exceptionOrNull()
        stillInterrupted = Thread.currentThread().isInterrupted
      }.join()

      Assertions.assertNull(thrown, "send should not throw for a thread that is interrupted")
      Assertions.assertTrue(stillInterrupted, "send should keep the interrupt status of the thread")
      Assertions.assertEquals("after interrupt", readLines(channel, 1).single().event(), "the message should be written")
    }

  @Test
  @DisplayName("Should make a caller wait once the queued lines hold PENDING_CHARS characters, and keep the caller's interrupt status")
  fun sendWaitsForRoomWhenQueuedLinesAreFull() =
    withChannel { channel ->
      // The sender thread takes this line first and cannot finish writing it until the test reads the channel,
      // so the line keeps its room while the lines after it fill the rest.
      LogSender.send(largeMessage())
      val names = List(LogSender.PENDING_CHARS / LARGE_EVENT.length) { "waited $it" }
      var thrown: Throwable? = null
      var stillInterrupted = false
      val sender =
        thread {
          Thread.currentThread().interrupt()
          thrown = runCatching { names.forEach { LogSender.send(largeMessage(it)) } }.exceptionOrNull()
          stillInterrupted = Thread.currentThread().isInterrupted
        }
      awaitWaiting(sender)
      sender.interrupt()
      awaitWaiting(sender)

      val events = readLines(channel, names.size + 1).map { it.event().removeSuffix(LARGE_EVENT) }
      sender.join()
      Assertions.assertNull(thrown, "send should wait for room instead of throwing")
      Assertions.assertTrue(stillInterrupted, "send should keep the calling thread's interrupt status")
      Assertions.assertEquals(listOf("") + names, events, "every line should be written, in the order that the lines were sent")
    }

  @Test
  @DisplayName("Should write a line longer than PENDING_CHARS characters")
  fun writesLineLongerThanTheLimit() =
    withChannel { channel ->
      val event = "y".repeat(LogSender.PENDING_CHARS + 1)
      thread { LogSender.send(message(event)) }

      Assertions.assertEquals(event, readLines(channel, 1).single().event())
    }

  @Test
  @DisplayName("Should let configure queue the earlier messages without waiting for room")
  fun configureQueuesEarlierMessagesWithoutWaitingForRoom() {
    val queued = List(LogSender.PENDING_CHARS / LARGE_EVENT.length + 1) { "queued $it" }

    // The test reads the channel only after configure returns, so the test would hang if configure waited for room.
    withChannel(queued = queued.map { largeMessage(it) }) { channel ->
      Assertions.assertEquals(queued, readLines(channel, queued.size).map { it.event().removeSuffix(LARGE_EVENT) })
    }
  }

  @Test
  @DisplayName("Should write the messages queued before configure ahead of the messages that other threads send while configure runs")
  fun writesQueuedMessagesBeforeConcurrentSends() {
    LogSender.messages.clear()
    val queued = List(300) { "queued $it" }
    queued.forEach { LogSender.messages.add(message(it)) }
    val start = CountDownLatch(1)
    val senders =
      List(4) { sender ->
        thread {
          start.await()
          repeat(50) { LogSender.send(message("sent $sender-$it")) }
        }
      }
    val configurer =
      thread {
        start.await()
        LogSender.configure(channel)
      }
    start.countDown()
    (senders + configurer).forEach { it.join() }

    val events = readLines(channel, queued.size + 200).map { it.event() }
    Assertions.assertEquals(queued, events.take(queued.size), "the messages queued before configure should come first, in order")
  }

  @Test
  @DisplayName("Should give back a failed line's room and keep writing when printing the write error throws")
  fun keepsWritingAfterErrorReportThrows() {
    val originalStderr = System.err
    System.setErr(
      object : PrintStream(OutputStream.nullOutputStream()) {
        override fun println(x: String?): Unit = throw IllegalStateException("stderr is closed")
      },
    )
    try {
      val broken = ByteChannel(autoFlush = true)
      LogSender.messages.clear()
      LogSender.configure(broken)
      // This line takes all the room, so no later line could be queued if its room were not given back.
      LogSender.send(message("z".repeat(LogSender.PENDING_CHARS)))
      broken.cancel(IOException("closed by the test"))
      LogSender.flush()
    } finally {
      System.setErr(originalStderr)
    }

    withChannel { channel ->
      LogSender.send(message("after the error"))
      Assertions.assertEquals("after the error", readLines(channel, 1).single().event())
    }
  }

  @Test
  @DisplayName("Should keep writing after the sender thread is interrupted")
  fun keepsWritingAfterSenderThreadIsInterrupted() =
    withChannel { channel ->
      val sender = Thread.getAllStackTraces().keys.single { it.name == "Airflow log sender" }
      LogSender.flush()
      awaitWaiting(sender)
      sender.interrupt()
      LogSender.send(message("after the interrupt"))

      Assertions.assertEquals("after the interrupt", readLines(channel, 1).single().event())
    }

  @Test
  @DisplayName("Should make detachAndFlush also wait for a line that another thread started to send before the call")
  fun detachAndFlushWaitsForLineBeingSent() =
    withChannel { channel ->
      // The first line holds its room until the test reads that line from the channel, and the fillers take
      // the rest of the room. So the sender below waits for room while it holds LogSender's lock.
      LogSender.send(largeMessage("first"))
      val fillers = List(LogSender.PENDING_CHARS / LARGE_EVENT.length - 2) { "filler $it" }
      fillers.forEach { LogSender.send(largeMessage(it)) }
      val sender = thread { LogSender.send(largeMessage("sent")) }
      awaitWaiting(sender)
      val detacher = thread { LogSender.detachAndFlush() }

      val written = readLines(channel, 1 + fillers.size).map { it.event().removeSuffix(LARGE_EVENT) }
      detacher.join(TimeUnit.SECONDS.toMillis(1))
      Assertions.assertTrue(detacher.isAlive, "detachAndFlush should wait for the line that was being sent")
      Assertions.assertEquals("sent", readLines(channel, 1).single().event().removeSuffix(LARGE_EVENT))
      detacher.join()
      Assertions.assertEquals(listOf("first") + fillers, written)
    }

  @Test
  @DisplayName("Should write messages sent before detachAndFlush and keep messages sent after detachAndFlush in LogSender.messages")
  fun detachAndFlushKeepsLaterMessagesQueued() =
    withChannel { channel ->
      LogSender.send(message("before"))
      LogSender.detachAndFlush()
      LogSender.send(message("after"))

      Assertions.assertEquals("before", readLines(channel, 1).single().event())
      Assertions.assertEquals(
        listOf("after"),
        LogSender.messages.map { it.event },
        "a message sent after detachAndFlush should stay in LogSender.messages",
      )
      Assertions.assertEquals(0, channel.availableForRead, "nothing else should be written")
    }

  @Test
  @DisplayName("Should make flush wait until the queued lines are written, even on an interrupted thread")
  fun flushWaitsForQueuedLines() =
    withChannel { channel ->
      LogSender.send(largeMessage())
      var stillInterrupted = false
      val flusher =
        thread {
          Thread.currentThread().interrupt()
          LogSender.flush()
          stillInterrupted = Thread.currentThread().isInterrupted
        }
      awaitWaiting(flusher)

      readLines(channel, 1)
      flusher.join()
      Assertions.assertTrue(stillInterrupted, "flush should keep the calling thread's interrupt status")
    }

  @Test
  @DisplayName("Should write the arguments as they were when the message was sent")
  fun writesArgumentsAsTheyWereWhenSent() =
    withChannel { channel ->
      LogSender.send(largeMessage())
      val argument = StringBuilder("when sent")
      LogSender.send(LogMessage("snapshot", mapOf("value" to argument), "test", Level.INFO))
      argument.replace(0, argument.length, "changed later")

      val line = readLines(channel, 2).last()
      Assertions.assertEquals("when sent", line["value"]!!.jsonPrimitive.content)
    }

  @Test
  @DisplayName("Should not run coroutines that are ready on the calling thread while configure writes queued messages")
  fun configureRunsNoOtherCoroutine() =
    assertWriteRunsNoOtherCoroutine(configured = false) { channel ->
      LogSender.messages.add(message("queued"))
      LogSender.configure(channel)
    }

  @Test
  @DisplayName("Should not run coroutines that are ready on the calling thread while send writes a message")
  fun sendRunsNoOtherCoroutine() =
    assertWriteRunsNoOtherCoroutine(configured = true) {
      LogSender.send(message("sent"))
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
    LogSender.messages.clear()
    var other: Thread? = null
    try {
      if (configured) LogSender.configure(channel)
      val otherThreadLogged =
        runBlocking {
          val waiting =
            async {
              val logging = thread { LogSender.send(message("from another thread")) }
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
    }
  }

  /** Adds [queued] to [LogSender.messages], configures [LogSender] with this test's channel, and runs [block] with the channel. */
  private fun withChannel(
    queued: List<LogMessage> = emptyList(),
    block: (channel: ByteChannel) -> Unit,
  ) {
    LogSender.messages.clear()
    LogSender.messages.addAll(queued)
    LogSender.configure(channel)
    block(channel)
  }

  private fun readLines(
    channel: ByteChannel,
    count: Int,
  ): List<JsonObject> = runBlocking { List(count) { Json.parseToJsonElement(channel.readLine()!!).jsonObject } }

  /** Waits until [thread] parks, and fails if it ends instead. */
  private fun awaitWaiting(thread: Thread) {
    while (thread.state != Thread.State.WAITING) {
      Assertions.assertNotEquals(Thread.State.TERMINATED, thread.state, "the thread should wait instead of returning")
      Thread.sleep(10)
    }
  }

  private fun JsonObject.event(): String = this["event"]!!.jsonPrimitive.content

  private fun message(event: String) = LogMessage(event, emptyMap(), "test", Level.INFO)

  /**
   * A message whose event is [name] followed by [LARGE_EVENT]. The message does not fit in a ByteChannel's 1 MiB buffer, so it is not
   * fully written until the test reads the channel.
   */
  private fun largeMessage(name: String = "") = message(name + LARGE_EVENT)

  private companion object {
    val LARGE_EVENT = "x".repeat(2 * 1024 * 1024)
  }
}
