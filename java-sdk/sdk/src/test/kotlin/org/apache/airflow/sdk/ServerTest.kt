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

package org.apache.airflow.sdk

import io.ktor.network.sockets.InetSocketAddress
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.Frame
import org.apache.airflow.sdk.execution.IncomingFrame
import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import org.apache.airflow.sdk.execution.LogMessage
import org.apache.airflow.sdk.execution.LogSender
import org.apache.airflow.sdk.execution.RawFrame
import org.apache.airflow.sdk.execution.comm.GetVariable
import org.apache.airflow.sdk.execution.comm.SucceedTask
import org.apache.airflow.sdk.execution.comm.TaskState
import org.apache.airflow.sdk.execution.pollSignals
import org.apache.airflow.sdk.execution.withSignalRecorder
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.msgpack.core.MessagePack
import org.msgpack.core.buffer.ArrayBufferInput
import sun.misc.Signal
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

private class ServerNoopTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** What the tasks below report to the test that runs them. */
private val taskEvents = LinkedBlockingQueue<String>()

/** Releases [NeverCheckingTask]. */
private val releaseTask = LinkedBlockingQueue<String>()

/**
 * Waits for a stop request and records what its context showed. Tests use this task to signal the
 * JVM while a task is running.
 */
class StopAwareTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    // Check before reporting "started". The test raises SIGTERM as soon as it reads "started", so by
    // then the task has checked and the runtime keeps the signal.
    taskEvents.put("isCancelled() returned ${context.isCancelled()}")
    taskEvents.put("started")
    taskEvents.put("awaitCancellation returned ${context.awaitCancellation(Duration.ofSeconds(20))}")
    taskEvents.put("isCancelled() returned ${context.isCancelled()}")
  }
}

/** The copy that [CopyCheckingTask] checked, kept so the test can check it again after the task returns. */
@Volatile
private var checkedCopy: Context? = null

/**
 * This task works like [StopAwareTask], but it checks for a stop request only through a copy of its
 * context that another thread made.
 */
class CopyCheckingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    thread { checkedCopy = context.copy() }.join()
    val copy = checkNotNull(checkedCopy)
    taskEvents.put("isCancelled() returned ${copy.isCancelled()}")
    taskEvents.put("started")
    taskEvents.put("awaitCancellation returned ${copy.awaitCancellation(Duration.ofSeconds(20))}")
  }
}

class CheckOnceTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("isCancelled() returned ${context.isCancelled()}")
  }
}

/** Runs until the test releases it, without ever checking for a stop request. */
class NeverCheckingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    releaseTask.poll(20, TimeUnit.SECONDS)
  }
}

/** Gets a variable, so the task waits for a reply from the supervisor. */
class VariableTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    client.getVariable("my_variable")
  }
}

/**
 * Main class of the JVM that [ServerTest.taskGetsReplyWhileQueuedLogsAreWritten] starts. Its main
 * method turns on debug logging, queues 32 MiB of log messages before the log connection opens, and
 * serves [VariableTask] as task "a" of Dag "c".
 */
object QueuedLogsProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    Log.globalThreshold = Level.DEBUG
    Log.namedThresholds = emptyMap()
    repeat(64) { LogSender.messages.add(LogMessage("x".repeat(512 * 1024), emptyMap(), "probe", Level.INFO)) }
    Server.create(args).serve(Bundle(listOf(DagDef("c").addTask(TaskDef("a", VariableTask::class.java)))))
  }
}

class ServerTest {
  private fun hexToBytes(hex: String): ByteArray =
    hex
      .split(' ', '\r', '\n')
      .filter { it.isNotEmpty() }
      .map { it.toUByte(16).toByte() }
      .toByteArray()

  private fun ackFrame(id: Int): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(2)
      packer.packInt(id)
      packer.packNil()
    }
    return out.toByteArray()
  }

  private suspend fun ByteChannel.writeFrame(payload: ByteArray) {
    writeByteArray(Frame.lengthPrefix(payload.size.toUInt()))
    writeByteArray(payload)
  }

  private fun DataOutputStream.writeFrame(payload: ByteArray) {
    write(Frame.lengthPrefix(payload.size.toUInt()))
    write(payload)
    flush()
  }

  private fun DataInputStream.readFrame(): IncomingFrame {
    val prefix = ByteArray(4).also { readFully(it) }
    return CoordinatorComm.decode(ByteArray(Frame.parseLengthPrefix(prefix).toInt()).also { readFully(it) })
  }

  private fun variableResultFrame(id: Int): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(2)
      packer.packInt(id)
      packer.packMapHeader(3)
      packer.packString("type").packString("VariableResult")
      packer.packString("key").packString("my_variable")
      packer.packString("value").packString("value")
    }
    return out.toByteArray()
  }

  @Test
  @DisplayName("Should give a task its reply when the startup details arrive while queued logs are still being written")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun taskGetsReplyWhileQueuedLogsAreWritten() {
    val loopback = InetAddress.getLoopbackAddress()
    ServerSocket(0, 1, loopback).use { commListener ->
      ServerSocket(0, 1, loopback).use { logsListener ->
        commListener.soTimeout = 30_000
        logsListener.soTimeout = 30_000
        val process =
          ProcessBuilder(
            File(System.getProperty("java.home"), "bin/java").path,
            "-cp",
            System.getProperty("java.class.path"),
            QueuedLogsProbe::class.java.name,
            "--comm",
            "${loopback.hostAddress}:${commListener.localPort}",
            "--logs",
            "${loopback.hostAddress}:${logsListener.localPort}",
          ).redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start()
        try {
          commListener.accept().use { comm ->
            logsListener.accept().use { logs ->
              comm.soTimeout = 30_000
              logs.soTimeout = 30_000
              val toServer = DataOutputStream(comm.getOutputStream())
              val fromServer = DataInputStream(comm.getInputStream())
              // The first byte shows that the server has started to write the queued messages. The test
              // then reads no logs for a second, so the server is still writing the queued messages when
              // the startup details arrive.
              logs.getInputStream().read()
              toServer.writeFrame(hexToBytes(STARTUP_HEX))
              Thread.sleep(1000)
              thread(isDaemon = true) { runCatching { logs.getInputStream().transferTo(OutputStream.nullOutputStream()) } }

              val request = fromServer.readFrame()
              Assertions.assertInstanceOf(GetVariable::class.java, request.body)
              toServer.writeFrame(variableResultFrame(request.id))
              val result = fromServer.readFrame()
              Assertions.assertInstanceOf(SucceedTask::class.java, result.body, "the task should get its reply and succeed")
              toServer.writeFrame(ackFrame(result.id))
              Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the JVM should exit after it reports the result")
              Assertions.assertEquals(0, process.exitValue())
            }
          }
        } finally {
          process.destroyForcibly()
        }
      }
    }
  }

  @Test
  @DisplayName("Should run the task and report the result as a normal awaited request")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun runsTaskAndReportsResultAsAwaitedRequest() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))

    val reported = ArrayBlockingQueue<IncomingFrame>(1)
    val supervisor =
      Thread {
        runBlocking {
          // Deliver the StartupDetails frame (id 2, dag_id "c", task_id "a").
          toServer.writeFrame(hexToBytes(STARTUP_HEX))
          val prefix = fromServer.readByteArray(4)
          val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
          val result = CoordinatorComm.decode(payload)
          reported.put(result)
          toServer.writeFrame(ackFrame(result.id))
        }
      }
    supervisor.start()

    runBlocking { server.dispatchTask(Bundle(emptyList()), comm) }
    supervisor.join()

    val result = reported.take()
    Assertions.assertEquals(0, result.id)
    Assertions.assertInstanceOf(TaskState::class.java, result.body)
    Assertions.assertEquals(TaskState.State.REMOVED, (result.body as TaskState).state)
    comm.close()
  }

  /**
   * Runs [taskClass] as task "a" of Dag "c" through [CoordinatorServer.dispatchTask] and returns the
   * result that the task reports. A thread that plays the supervisor sends the startup details, calls
   * [whileRunning], reads the result, and calls [beforeAck] before it acknowledges the result. With
   * [ack] set to false, that thread closes the connection instead, so reporting the result fails.
   */
  private fun dispatchTask(
    taskClass: Class<out Task>,
    whileRunning: () -> Unit = {},
    beforeAck: () -> Unit = {},
    ack: Boolean = true,
  ): IncomingFrame {
    taskEvents.clear()
    LogSender.messages.clear()
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))
    val reported = ArrayBlockingQueue<IncomingFrame>(1)
    val supervisor =
      thread(isDaemon = true) {
        runBlocking {
          // Deliver the StartupDetails frame (id 2, dag_id "c", task_id "a").
          toServer.writeFrame(hexToBytes(STARTUP_HEX))
          whileRunning()
          val prefix = fromServer.readByteArray(4)
          val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
          val result = CoordinatorComm.decode(payload)
          reported.put(result)
          beforeAck()
          if (ack) toServer.writeFrame(ackFrame(result.id)) else comm.close()
        }
      }

    try {
      runBlocking { server.dispatchTask(Bundle(listOf(DagDef("c").addTask(TaskDef("a", taskClass)))), comm) }
    } finally {
      supervisor.join()
      comm.close()
    }
    return reported.take()
  }

  /** The events the runtime logged for the stop signals it got, in order. */
  private fun signalLogs(): List<String> = LogSender.messages.map { it.event }.filter { it in SIGNAL_EVENTS }

  /**
   * Waits until the runtime has logged [count] stop signals. By then the runtime has decided what to
   * do with each signal.
   */
  private fun awaitSignalLogs(count: Int) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (signalLogs().size < count && System.nanoTime() < deadline) Thread.sleep(10)
  }

  /** Waits until the running task reports [event], and drops any events the task reported earlier. */
  private fun awaitTaskEvent(event: String) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    while (System.nanoTime() < deadline && taskEvents.poll(100, TimeUnit.MILLISECONDS) != event) Unit
  }

  /** Waits until [count] SIGTERM handler threads are blocked, for example on a lock the test holds. */
  private fun awaitBlockedSignalHandlers(count: Int) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (System.nanoTime() < deadline) {
      val blocked = Thread.getAllStackTraces().keys.count { it.name == "SIGTERM handler" && it.state == Thread.State.BLOCKED }
      if (blocked >= count) return
      Thread.sleep(10)
    }
  }

  @Test
  @DisplayName("Should let a task that waits for a stop request see SIGTERM, and leave a second SIGTERM to the JVM")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun runningTaskSeesStopSignal() =
    withSignalRecorder("TERM") { recorded ->
      val result =
        dispatchTask(
          StopAwareTask::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
          beforeAck = {
            Signal.raise(Signal("TERM"))
            awaitSignalLogs(2)
          },
        )

      Assertions.assertEquals(
        listOf("awaitCancellation returned true", "isCancelled() returned true"),
        taskEvents.toList(),
        "the task should see the first SIGTERM through its context",
      )
      Assertions.assertInstanceOf(SucceedTask::class.java, result.body, "the task returned, so its success is reported")
      Assertions.assertEquals(listOf("Asking the task to stop", "Letting the JVM handle the signal"), signalLogs())
      Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "the second SIGTERM should reach the JVM's handler")
    }

  @Test
  @DisplayName(
    "Should let a task that checks for a stop request only through a copy of its context see SIGTERM, " +
      "and let the copy still see it after the task returns",
  )
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun copiedContextSeesStopSignal() =
    withSignalRecorder("TERM") { recorded ->
      checkedCopy = null
      dispatchTask(
        CopyCheckingTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
        },
      )

      Assertions.assertEquals(listOf("awaitCancellation returned true"), taskEvents.toList(), "the copy should see the SIGTERM")
      Assertions.assertEquals(listOf("Asking the task to stop"), signalLogs())
      Assertions.assertNull(recorded.poll(), "the SDK should keep the SIGTERM for the task")
      val copy = checkNotNull(checkedCopy)
      Assertions.assertTrue(copy.isCancelled(), "the copy should still see the stop request after the task returns")
      Assertions.assertFalse(copy.copy().isCancelled(), "a context made after the task returns should not see the stop request")
    }

  @Test
  @DisplayName("Should not show the stop request to a context made after the task when reporting the result fails")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun stopRequestEndsWhenReportingFails() =
    withSignalRecorder("TERM") { _ ->
      Assertions.assertThrows(ApiError::class.java) {
        dispatchTask(
          StopAwareTask::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
          ack = false,
        )
      }

      Assertions.assertEquals(listOf("Asking the task to stop"), signalLogs())
      Assertions.assertFalse(taskContext().isCancelled(), "a context made after the failed report should not see the stop request")
    }

  @Test
  @DisplayName("Should hold the first SIGTERM until the result is reported when the task checked for a stop request")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalWhileReportingIsHeld() =
    withSignalRecorder("TERM") { recorded ->
      val result =
        dispatchTask(
          CheckOnceTask::class.java,
          beforeAck = {
            // The runtime is still waiting for this acknowledgement, so it has not finished reporting yet.
            Signal.raise(Signal("TERM"))
            awaitSignalLogs(1)
          },
        )

      Assertions.assertInstanceOf(SucceedTask::class.java, result.body)
      Assertions.assertEquals(listOf("Asking the task to stop"), signalLogs())
      Assertions.assertNull(recorded.poll(), "SIGTERM should reach the runtime until it has reported the result")
      Signal.raise(Signal("TERM"))
      Assertions.assertEquals(
        listOf("SIGTERM"),
        pollSignals(recorded, 1),
        "after the result is reported, SIGTERM should go back to the handler that was there before",
      )
    }

  @Test
  @DisplayName("Should leave SIGTERM to the JVM when the task never checks for a stop request")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalGoesToTheJvmWhenTaskNeverChecks() =
    withSignalRecorder("TERM") { recorded ->
      val handedBack = LinkedBlockingQueue<String>()
      dispatchTask(
        NeverCheckingTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
          handedBack.put(recorded.poll(10, TimeUnit.SECONDS) ?: "nothing")
          releaseTask.put("done")
        },
      )

      Assertions.assertEquals("SIGTERM", handedBack.take(), "SIGTERM should reach the JVM's handler while the task runs")
      Assertions.assertEquals(listOf("Letting the JVM handle the signal"), signalLogs())
    }

  @Test
  @DisplayName("Should take only one of two SIGTERMs that arrive together, and leave the other to the JVM")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun takesOnlyOneOfTwoSignals() =
    withSignalRecorder("TERM") { recorded ->
      dispatchTask(
        StopAwareTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          // Each handler decides whether to take its signal and then logs, which needs this lock.
          // Holding the lock stops both handlers after they decide and before they finish.
          synchronized(LogSender) {
            Signal.raise(Signal("TERM"))
            Signal.raise(Signal("TERM"))
            awaitBlockedSignalHandlers(2)
          }
        },
      )

      Assertions.assertEquals(
        listOf("Asking the task to stop", "Letting the JVM handle the signal"),
        signalLogs().sorted(),
        "one SIGTERM should be taken for the task and the other left to the JVM",
      )
      Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "one SIGTERM should reach the JVM's handler")
    }

  @Test
  @DisplayName("Should fail when the initial frame is not startup details")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun failsWhenInitialFrameIsNotStartupDetails() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))

    Assertions.assertThrows(ApiError::class.java) {
      runBlocking {
        toServer.writeFrame(ackFrame(7))
        server.dispatchTask(Bundle(emptyList()), comm)
      }
    }
    comm.close()
  }

  @Test
  @DisplayName("Should serialize the bundle's dags when the initial frame is a parse request")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun parsesDagsAndReportsResult() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))
    val bundle = Bundle(listOf(DagDef("parsed_dag").addTask(TaskDef("t", ServerNoopTask::class.java))))

    val reported = ArrayBlockingQueue<RawFrame>(1)
    val supervisor =
      Thread {
        runBlocking {
          toServer.writeFrame(parseRequestFrame(3, "/bundle/dags/java.py", "/bundle"))
          val prefix = fromServer.readByteArray(4)
          val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
          val raw = Frame.decodeRaw(ArrayBufferInput(payload))
          reported.put(raw)
          toServer.writeFrame(ackFrame(raw.id))
        }
      }
    supervisor.start()

    runBlocking { server.dispatchTask(bundle, comm) }
    supervisor.join()

    val body = reported.take().rawBody as Map<*, *>
    Assertions.assertEquals("DagFileParsingResult", body["type"])
    Assertions.assertEquals("/bundle/dags/java.py", body["fileloc"])
    val dags = body["serialized_dags"] as List<*>
    Assertions.assertEquals(1, dags.size)
    val dag = ((dags[0] as Map<*, *>)["data"] as Map<*, *>)["dag"] as Map<*, *>
    Assertions.assertEquals("parsed_dag", dag["dag_id"])
    comm.close()
  }

  private fun parseRequestFrame(
    id: Int,
    file: String,
    bundlePath: String,
  ): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(2)
      packer.packInt(id)
      packer.packMapHeader(3)
      packer.packString("type").packString("DagFileParseRequest")
      packer.packString("file").packString(file)
      packer.packString("bundle_path").packString(bundlePath)
    }
    return out.toByteArray()
  }

  private companion object {
    val SIGNAL_EVENTS = setOf("Asking the task to stop", "Letting the JVM handle the signal")

    // [2, msg, null] with msg coming from
    // https://github.com/astronomer/airflow/blob/f39c8da8/task-sdk/tests/task_sdk/execution_time/test_comms.py#L73-L108
    val STARTUP_HEX =
      """
      92 02 88 a4 74 79 70 65 ae 53 74 61 72 74 75 70 44 65 74 61 69 6c 73 a2 74 69 86 a2 69 64 d9 24
      34 64 38 32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63
      65 63 61 62 a7 74 61 73 6b 5f 69 64 a1 61 aa 74 72 79 5f 6e 75 6d 62 65 72 01 a6 72 75 6e 5f 69
      64 a1 62 a6 64 61 67 5f 69 64 a1 63 ae 64 61 67 5f 76 65 72 73 69 6f 6e 5f 69 64 d9 24 34 64 38
      32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63 65 63 61
      62 aa 74 69 5f 63 6f 6e 74 65 78 74 85 a7 64 61 67 5f 72 75 6e 8c a6 64 61 67 5f 69 64 a1 63 a6
      72 75 6e 5f 69 64 a1 62 ac 6c 6f 67 69 63 61 6c 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31
      54 30 31 3a 30 30 3a 30 30 5a b3 64 61 74 61 5f 69 6e 74 65 72 76 61 6c 5f 73 74 61 72 74 b4 32
      30 32 34 2d 31 32 2d 30 31 54 30 30 3a 30 30 3a 30 30 5a b1 64 61 74 61 5f 69 6e 74 65 72 76 61
      6c 5f 65 6e 64 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a aa 73 74 61 72 74
      5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a9 72 75 6e 5f 61
      66 74 65 72 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a8 65 6e 64 5f 64 61
      74 65 c0 a8 72 75 6e 5f 74 79 70 65 a6 6d 61 6e 75 61 6c a5 73 74 61 74 65 a7 73 75 63 63 65 73
      73 a4 63 6f 6e 66 c0 b5 63 6f 6e 73 75 6d 65 64 5f 61 73 73 65 74 5f 65 76 65 6e 74 73 90 a9 6d
      61 78 5f 74 72 69 65 73 00 ac 73 68 6f 75 6c 64 5f 72 65 74 72 79 c2 a9 76 61 72 69 61 62 6c 65
      73 c0 ab 63 6f 6e 6e 65 63 74 69 6f 6e 73 c0 a4 66 69 6c 65 a9 2f 64 65 76 2f 6e 75 6c 6c aa 73
      74 61 72 74 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a ac 64
      61 67 5f 72 65 6c 5f 70 61 74 68 a9 2f 64 65 76 2f 6e 75 6c 6c ab 62 75 6e 64 6c 65 5f 69 6e 66
      6f 82 a4 6e 61 6d 65 a8 61 6e 79 2d 6e 61 6d 65 a7 76 65 72 73 69 6f 6e ab 61 6e 79 2d 76 65 72
      73 69 6f 6e b2 73 65 6e 74 72 79 5f 69 6e 74 65 67 72 61 74 69 6f 6e a0 c0
      """.trimIndent()
  }
}
