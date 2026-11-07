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
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
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
  @DisplayName("Should report only the first failed write to a channel instead of throwing its error to the caller")
  fun reportsFirstFailedWriteOfChannel() {
    val stderr = ByteArrayOutputStream()
    withErrors(PrintStream(stderr, true)) {
      withChannel { channel ->
        repeat(3) { LogSender.send(largeMessage("line $it")) }
        channel.cancel(IOException("closed by the test"))
        LogSender.flush()
      }
    }

    val reports = stderr.toString().lines().filter { it.startsWith("Cannot send a log line to the supervisor: ") }
    Assertions.assertEquals(1, reports.size) { "stderr: $stderr" }
  }

  @Test
  @DisplayName("Should report a failed write while another thread holds the lock of System.err")
  fun reportsFailedWriteWithoutSystemErr() =
    withChannel { channel ->
      LogSender.send(largeMessage())
      val originalStderr = System.err
      val stderr = object : PrintStream(OutputStream.nullOutputStream(), true) {}
      val holding = CountDownLatch(1)
      val release = CountDownLatch(1)
      System.setErr(stderr)
      val holder =
        thread {
          // A thread that logs while it holds this lock, for example through a bridge that sends stderr to a logger,
          // can wait for room in LogSender. Only the sender thread gives that room back. Gradle also takes this lock
          // when it flushes System.err between tests. If this test timed out, its finally block might never run. So
          // this thread holds the lock only for a limited time.
          synchronized(stderr) {
            holding.countDown()
            release.await(20, TimeUnit.SECONDS)
          }
        }
      try {
        holding.await()
        channel.cancel(IOException("closed by the test"))
        val flusher = thread { LogSender.flush() }
        flusher.join(TimeUnit.SECONDS.toMillis(10))
        Assertions.assertFalse(flusher.isAlive, "the sender thread should report the failed write without using System.err")
      } finally {
        release.countDown()
        holder.join()
        System.setErr(originalStderr)
      }
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
    val throwing =
      object : PrintStream(OutputStream.nullOutputStream()) {
        override fun println(x: String?): Unit = throw IllegalStateException("stderr is closed")
      }
    withErrors(throwing) {
      val broken = ByteChannel(autoFlush = true)
      LogSender.messages.clear()
      LogSender.configure(broken)
      // This line takes all the room, so no later line could be queued if its room were not given back.
      LogSender.send(message("z".repeat(LogSender.PENDING_CHARS)))
      broken.cancel(IOException("closed by the test"))
      LogSender.flush()
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
  @DisplayName("Should put a message in LogSender.messages, and give back its room, if the message waits for room during detachAndFlush")
  fun detachAndFlushKeepsWaitingMessageQueued() {
    withChannel { channel ->
      val fillers = fillRoom()
      val sender = thread { LogSender.send(largeMessage("waiting")) }
      awaitWaiting(sender)
      val detacher = thread { LogSender.detachAndFlush() }
      // The detacher thread waits for the queued lines only after it has stopped queuing new lines. So once
      // that thread waits, LogSender queues no more lines.
      awaitWaiting(detacher)

      val written = readLines(channel, 1 + fillers.size).map { it.event().removeSuffix(LARGE_EVENT) }
      detacher.join()
      sender.join()
      Assertions.assertEquals(listOf("first") + fillers, written)
      Assertions.assertEquals(listOf("waiting"), LogSender.messages.map { it.event.removeSuffix(LARGE_EVENT) })
      Assertions.assertEquals(0, channel.availableForRead, "nothing else should be written")
    }

    // The waiting message took room before LogSender put the message in LogSender.messages. If LogSender did not
    // give that room back, this line could not be queued, because it needs all the room.
    withChannel { channel ->
      thread { LogSender.send(message("z".repeat(LogSender.PENDING_CHARS))) }
      Assertions.assertEquals("zzzzz", readLines(channel, 1).single().event().take(5))
    }
  }

  @Test
  @DisplayName("Should put a message in LogSender.messages without waiting for room once detachAndFlush has stopped queuing")
  fun keepsMessageAfterDetachWithoutWaitingForRoom() =
    withChannel { channel ->
      val fillers = fillRoom()
      val waiting = thread { LogSender.send(largeMessage("waiting")) }
      awaitWaiting(waiting)
      val detacher = thread { LogSender.detachAndFlush() }
      awaitWaiting(detacher)

      // If this message asked for room, it would wait behind the waiting thread.
      val late = thread { LogSender.send(message("late")) }
      late.join(TimeUnit.SECONDS.toMillis(10))
      Assertions.assertFalse(late.isAlive, "a message that stays in LogSender.messages should not wait for room")

      readLines(channel, 1 + fillers.size)
      detacher.join()
      waiting.join()
      Assertions.assertEquals(listOf("late", "waiting"), LogSender.messages.map { it.event.removeSuffix(LARGE_EVENT) })
    }

  @Test
  @DisplayName("Should queue a line from a logger that does not wait for room, even when another thread is waiting for room")
  fun queuesLineWithoutWaitingForRoom() =
    withChannel { channel ->
      val fillers = fillRoom()
      val waiting = thread { LogSender.send(largeMessage("waiting")) }
      awaitWaiting(waiting)

      val quick = thread { Logger("test", waitsForRoom = false).error("quick") }
      quick.join(TimeUnit.SECONDS.toMillis(10))
      Assertions.assertFalse(quick.isAlive, "the line should be queued without waiting for room")

      val events = readLines(channel, fillers.size + 3).map { it.event().removeSuffix(LARGE_EVENT) }
      waiting.join()
      Assertions.assertEquals(listOf("first") + fillers + listOf("quick", "waiting"), events)
    }

  @Test
  @DisplayName("Should let the sender thread log while another thread waits for room")
  fun senderThreadLogsWhileAnotherThreadWaitsForRoom() {
    val next = ByteChannel(autoFlush = true)
    var logged = false
    // When the sender thread reports a failed write, this stream logs a message on the sender thread, as a
    // logging bridge could.
    val logging =
      object : PrintStream(OutputStream.nullOutputStream()) {
        override fun println(x: String?) {
          if (!logged) {
            logged = true
            LogSender.send(message("logged by the sender thread"))
          }
        }
      }
    try {
      withErrors(logging) {
        withChannel { channel ->
          fillRoom()
          // This thread waits for all the room. Once the queued lines fail, this thread gets all the room before any
          // later caller. So a line that the sender thread logs can go out only if the sender thread does not wait for
          // room.
          val waiting = thread { LogSender.send(message("waiting" + "w".repeat(LogSender.PENDING_CHARS))) }
          awaitWaiting(waiting)
          // From here on, LogSender queues lines for the next channel. Once the old channel is cancelled, the lines
          // queued for it fail. The sender thread then logs while it reports the failure.
          LogSender.configure(next)
          channel.cancel(IOException("closed by the test"))

          val events = readLines(next, 2).map { it.event().substringBefore("www") }
          waiting.join()
          Assertions.assertEquals(setOf("logged by the sender thread", "waiting"), events.toSet())
        }
      }
    } finally {
      next.cancel(null)
    }
  }

  @Test
  @DisplayName("Should give back the room of a failed line before the sender thread reports the failure")
  fun givesBackRoomBeforeReportingFailure() {
    val reporting = CountDownLatch(1)
    val release = CountDownLatch(1)
    // This stream blocks each report of a failed write, as a full stderr does while nothing reads it.
    val blocking =
      object : PrintStream(OutputStream.nullOutputStream()) {
        override fun println(x: String?) {
          reporting.countDown()
          release.await()
        }
      }
    withErrors(blocking) {
      try {
        withChannel { channel ->
          fillRoom()
          // This thread needs all the room, so it gets room only after every queued line has failed.
          val waiting = thread { LogSender.send(message("waiting" + "w".repeat(LogSender.PENDING_CHARS))) }
          awaitWaiting(waiting)
          channel.cancel(IOException("closed by the test"))

          waiting.join(TimeUnit.SECONDS.toMillis(10))
          Assertions.assertFalse(waiting.isAlive, "the waiting thread should get room although the report blocks")
          Assertions.assertTrue(reporting.await(10, TimeUnit.SECONDS), "the sender thread should still report the failure")
        }
      } finally {
        release.countDown()
      }
    }
  }

  @Test
  @DisplayName("Should make flush wait until the sender thread has printed the reports of failed writes")
  fun flushWaitsForReports() {
    val printed = AtomicBoolean()
    val slow =
      object : PrintStream(OutputStream.nullOutputStream()) {
        override fun println(x: String?) {
          Thread.sleep(500)
          printed.set(true)
        }
      }
    withErrors(slow) {
      withChannel { channel ->
        LogSender.send(largeMessage())
        // Nothing reads the channel, so the sender thread cannot finish writing this line until the test cancels the
        // channel. The task that flush queues waits behind this line. So the write of this line fails before the
        // sender thread runs that task. The test cancels the channel only after flush has queued its task. If the
        // test cancelled the channel earlier, the sender thread could run out of lines and print the report of the
        // failed write before flush queues its task. In that case, the test would pass even if flush did not wait for
        // the reports.
        val flusher = thread { LogSender.flush() }
        awaitWaiting(flusher)
        channel.cancel(IOException("closed by the test"))

        flusher.join()
        Assertions.assertTrue(printed.get(), "flush should return only after the report is printed")
      }
    }
  }

  @Test
  @DisplayName("Should make a short line wait for room behind a long line that asked for room first")
  fun shortLineWaitsBehindLongLine() =
    withChannel { channel ->
      // The first line holds its room until the test reads the line, so the long line that the test sends
      // later has to wait for room.
      LogSender.send(largeMessage("first"))
      val long = thread { LogSender.send(message("y".repeat(LogSender.PENDING_CHARS))) }
      awaitWaiting(long)
      // Enough room is free for this line, but the long line asked for room first.
      val short = thread { LogSender.send(message("short")) }
      awaitWaiting(short)

      Assertions.assertEquals(listOf("first", "yyyyy", "short"), readLines(channel, 3).map { it.event().take(5) })
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
  @DisplayName("Should keep flushing past the idle timeout while the sender thread keeps finishing lines")
  fun flushWaitsWhileLinesAreWritten() {
    val names = List(8) { "line $it" }
    withChannel(queued = names.map { largeMessage(it) }) { channel ->
      var allWritten = false
      val flusher = thread { allWritten = LogSender.flush(Duration.ofSeconds(3)) }
      // The test reads a line every half second, well within the idle timeout. Reading all the lines takes longer
      // than the idle timeout, so flush has to keep waiting while the sender thread finishes each line.
      val read =
        names.indices.map { index ->
          if (index == names.lastIndex) Assertions.assertTrue(flusher.isAlive, "flush should still wait for the last line")
          Thread.sleep(500)
          readLines(channel, 1).single().event().removeSuffix(LARGE_EVENT)
        }
      flusher.join()
      Assertions.assertEquals(names, read)
      Assertions.assertTrue(allWritten, "flush should report that every line was written")
    }
  }

  @Test
  @DisplayName("Should stop flushing once the sender thread has not finished a line for the whole idle timeout")
  fun flushStopsWhenNoLineIsWritten() =
    withChannel { channel ->
      LogSender.send(largeMessage())
      // The sender thread finished its last line more than one idle timeout ago, but flush counts the idle time from
      // the start of the call.
      Thread.sleep(1500)
      val started = System.nanoTime()
      val allWritten = LogSender.flush(Duration.ofSeconds(1))
      val elapsed = Duration.ofNanos(System.nanoTime() - started)

      Assertions.assertFalse(allWritten, "flush should report that it stopped waiting")
      Assertions.assertTrue(elapsed >= Duration.ofSeconds(1) && elapsed < Duration.ofSeconds(5)) { "flush took $elapsed" }
    }

  @Test
  @DisplayName("Should fail the remaining lines, and say why, once detachAndFlush stops waiting")
  fun detachAndFlushFailsRemainingLinesWhenItStopsWaiting() {
    val stderr = ByteArrayOutputStream()
    withErrors(PrintStream(stderr, true)) {
      withChannel { _ ->
        fillRoom()
        val waiting = thread { LogSender.send(largeMessage("waiting")) }
        awaitWaiting(waiting)
        Assertions.assertFalse(LogSender.detachAndFlush(Duration.ofSeconds(1)), "detachAndFlush should report that it stopped waiting")

        // Nothing reads the channel, so the waiting thread gets room back only if the queued lines fail.
        waiting.join(TimeUnit.SECONDS.toMillis(10))
        Assertions.assertFalse(waiting.isAlive, "the waiting thread should get room back")
        Assertions.assertEquals(listOf("waiting"), LogSender.messages.map { it.event.removeSuffix(LARGE_EVENT) })
      }
    }

    Assertions.assertTrue(stderr.toString().contains("no log line was written for 1000 ms")) { "stderr: $stderr" }
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

  @Test
  @DisplayName("Should cancel the channel when detachAndFlush stops waiting, even after an earlier call stopped queuing")
  fun detachAndFlushCancelsChannelAfterEarlierDetach() =
    withErrors(PrintStream(OutputStream.nullOutputStream())) {
      withChannel { _ ->
        LogSender.send(largeMessage())
        // Nothing reads the channel, so this call waits for the line until the channel is cancelled.
        val earlier = thread { LogSender.detachAndFlush() }
        awaitWaiting(earlier)

        Assertions.assertFalse(LogSender.detachAndFlush(Duration.ofSeconds(1)), "the second call should stop waiting")
        earlier.join(TimeUnit.SECONDS.toMillis(10))
        Assertions.assertFalse(earlier.isAlive, "cancelling the channel should end the earlier wait")
      }
    }

  /**
   * Sends a large first line and returns the names of the filler lines that it sends next. The first line holds its room until
   * the test reads that line. The filler lines take so much of the remaining room that the next large message has to wait for room.
   */
  private fun fillRoom(): List<String> {
    LogSender.send(largeMessage("first"))
    return List(LogSender.PENDING_CHARS / LARGE_EVENT.length - 2) { "filler $it" }.onEach { LogSender.send(largeMessage(it)) }
  }

  /** Runs [block] while the sender thread reports failed writes to [stream]. */
  private fun withErrors(
    stream: PrintStream,
    block: () -> Unit,
  ) {
    // A write that failed in an earlier test can leave a report that the sender thread has not printed yet. This flush
    // waits until the sender thread has printed that report, so that stream gets only the reports of this test.
    LogSender.flush(Duration.ofSeconds(10))
    val original = LogSender.errors
    LogSender.errors = stream
    try {
      block()
    } finally {
      // The sender thread may still be reporting a failed write, so wait until the sender thread is done before
      // restoring LogSender.errors.
      LogSender.flush(Duration.ofSeconds(10))
      LogSender.errors = original
    }
  }

  private fun readLines(
    channel: ByteChannel,
    count: Int,
  ): List<JsonObject> = runBlocking { List(count) { Json.parseToJsonElement(channel.readLine()!!).jsonObject } }

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

/** Waits until [thread] parks, and fails if it ends instead. */
internal fun awaitWaiting(thread: Thread) {
  while (thread.state != Thread.State.WAITING) {
    Assertions.assertNotEquals(Thread.State.TERMINATED, thread.state, "the thread should wait instead of returning")
    Thread.sleep(10)
  }
}

/**
 * Configures [LogSender] with [channel], which nothing reads, so that every later caller that asks for room waits.
 * A first line keeps its room because it cannot be fully written, and the returned thread waits for all the room.
 */
internal fun blockLogRoom(channel: ByteChannel): Thread {
  LogSender.messages.clear()
  LogSender.configure(channel)
  LogSender.send(LogMessage("x".repeat(2 * 1024 * 1024), emptyMap(), "test", Level.INFO))
  return thread { LogSender.send(LogMessage("y".repeat(LogSender.PENDING_CHARS), emptyMap(), "test", Level.INFO)) }
    .also { awaitWaiting(it) }
}

/**
 * Undoes [blockLogRoom]. This helper closes [channel], so that the queued lines fail and give their room back. Then the
 * helper waits for [waiting].
 */
internal fun releaseLogRoom(
  channel: ByteChannel,
  waiting: Thread,
) {
  channel.cancel(null)
  waiting.join()
  LogSender.flush(Duration.ofSeconds(10))
  LogSender.messages.clear()
}
