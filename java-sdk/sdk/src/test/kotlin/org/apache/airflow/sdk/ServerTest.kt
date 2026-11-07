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
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
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
import java.io.InputStream
import java.io.OutputStream
import java.io.PrintStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private class ServerNoopTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

/** What the tasks below report to the test that runs them. */
private val taskEvents = LinkedBlockingQueue<String>()

/** Releases a task that waits for the test. */
private val releaseTask = LinkedBlockingQueue<String>()

/** Releases a [StoppableTask.onKill] call that waits for the test. */
private val releaseHook = LinkedBlockingQueue<String>()

/**
 * Waits for a stop request, and records what its context showed. It reports "started" before it first
 * looks at its context, so the test raises SIGTERM at a moment when the task has not checked for a
 * stop request yet.
 */
class StopAwareTask : StoppableTask {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    taskEvents.put("awaitCancellation returned ${context.awaitCancellation(Duration.ofSeconds(20))}")
    taskEvents.put("isCancelled() returned ${context.isCancelled()}")
  }

  override fun onKill(context: Context) = Unit
}

/** Blocks without looking at its context, so only [onKill] can make it return. */
class BlockedTask : StoppableTask {
  private val released = CountDownLatch(1)

  @Volatile
  private var taskContext: Context? = null

  @Volatile
  private var taskClassLoader: ClassLoader? = null

  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskContext = context
    taskClassLoader = Thread.currentThread().contextClassLoader
    taskEvents.put("started")
    check(released.await(20, TimeUnit.SECONDS)) { "onKill did not run" }
    taskEvents.put("execute returned")
  }

  override fun onKill(context: Context) {
    taskEvents.put("onKill got the context of execute: ${context === taskContext}")
    taskEvents.put("onKill got the class loader of execute: ${Thread.currentThread().contextClassLoader === taskClassLoader}")
    taskEvents.put("onKill saw isCancelled() return ${context.isCancelled()}")
    released.countDown()
  }
}

/** Returns as soon as the task starts. */
class ReturningTask : StoppableTask {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit

  override fun onKill(context: Context) {
    taskEvents.put("onKill ran")
  }
}

/** Returns while its [onKill] is still running. */
class ReturnsDuringOnKillTask : StoppableTask {
  private val hookStarted = CountDownLatch(1)
  private val executeReturning = CountDownLatch(1)

  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    check(hookStarted.await(20, TimeUnit.SECONDS)) { "onKill did not run" }
    taskEvents.put("execute returned")
    executeReturning.countDown()
  }

  override fun onKill(context: Context) {
    hookStarted.countDown()
    check(executeReturning.await(20, TimeUnit.SECONDS)) { "execute did not return" }
    // If a runtime does not wait for this method, this sleep gives that runtime enough time to report the
    // task's result first.
    Thread.sleep(300)
    taskEvents.put("onKill returned")
  }
}

/** Runs until the test releases it. Its [onKill] also runs until the test releases it. */
class HangingOnKillTask : StoppableTask {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    releaseTask.poll(20, TimeUnit.SECONDS)
  }

  override fun onKill(context: Context) {
    taskEvents.put("onKill started")
    releaseHook.poll(20, TimeUnit.SECONDS)
    taskEvents.put("onKill returned")
  }
}

/** Runs until the test releases it. Its [onKill] fails. */
class FailingOnKillTask : StoppableTask {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    releaseTask.poll(20, TimeUnit.SECONDS)
  }

  override fun onKill(context: Context): Unit = throw IllegalStateException("the job cannot be cancelled")
}

/** Blocks on a future that [onKill] cancels. [onKill] keeps working after it cancels the future. */
class CancelledFutureTask : StoppableTask {
  private val future = CompletableFuture<Unit>()

  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    future.get(20, TimeUnit.SECONDS)
  }

  override fun onKill(context: Context) {
    future.cancel(true)
    // If a runtime treats a cancelled future as its own cancellation, this sleep gives that runtime enough
    // time to exit first.
    Thread.sleep(300)
    taskEvents.put("onKill returned")
  }
}

/** Sleeps until [onKill] interrupts the task's thread, then restores the interrupt flag as the task fails. */
class InterruptedTask : StoppableTask {
  @Volatile
  private var taskThread: Thread? = null

  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskThread = Thread.currentThread()
    taskEvents.put("started")
    try {
      Thread.sleep(20_000)
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
      throw IllegalStateException("Airflow asked the task to stop", e)
    }
  }

  override fun onKill(context: Context) {
    checkNotNull(taskThread).interrupt()
  }
}

/** A condition that blocks until [onKill] releases it, and fails instead of deciding. */
class BlockedCondition :
  ConditionTask,
  StoppableTask {
  private val released = CountDownLatch(1)

  override fun decide(
    context: Context,
    client: Client,
  ): Boolean {
    taskEvents.put("started")
    check(released.await(20, TimeUnit.SECONDS)) { "onKill did not run" }
    throw IllegalStateException("Airflow asked the task to stop")
  }

  override fun onKill(context: Context) {
    taskEvents.put("onKill ran")
    released.countDown()
  }
}

/** Where the JVM's handler records signals in the test that runs [SignalInConstructorTask]. */
@Volatile
private var jvmSignals: BlockingQueue<String>? = null

/** Raises SIGTERM while the SDK makes an instance of this class, which is before the SDK starts the task. */
class SignalInConstructorTask : StoppableTask {
  init {
    Signal.raise(Signal("TERM"))
    taskEvents.put("the JVM's handler got ${jvmSignals?.poll(10, TimeUnit.SECONDS)}")
  }

  override fun execute(
    context: Context,
    client: Client,
  ) = Unit

  override fun onKill(context: Context) {
    taskEvents.put("onKill ran")
  }
}

/** Runs until the test releases it. */
class PlainBlockedTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    taskEvents.put("started")
    releaseTask.poll(20, TimeUnit.SECONDS)
  }
}

/** Checks for a stop request, which a plain [Task] cannot see. */
class PlainCheckingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    context.isCancelled()
  }
}

/** Waits briefly for a stop request, which a plain [Task] cannot see. */
class PlainWaitingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    context.awaitCancellation(Duration.ofMillis(1))
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
    repeat(QUEUED_LINES) { LogSender.messages.add(LogMessage(QUEUED_EVENT, emptyMap(), "probe", Level.INFO)) }
    Server.create(args).serve(Bundle(listOf(DagDef("c").addTask(TaskDef("a", VariableTask::class.java)))))
  }
}

/**
 * [ServerTest.exitsOnSigtermWhileNothingReadsTheLogs] starts a JVM with this object as its main class. The main method
 * of this object first sets [logFlushTimeout], which limits how long the SDK's shutdown hook waits, to 2 seconds. Then
 * the method does what [QueuedLogsProbe] does.
 */
object UnreadLogsProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    logFlushTimeout = Duration.ofSeconds(2)
    QueuedLogsProbe.main(args)
  }
}

/** Logs 32 MiB of messages, and then makes the JVM exit with code 3. */
class ExitingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    repeat(QUEUED_LINES) { LogSender.send(LogMessage(QUEUED_EVENT, emptyMap(), "probe", Level.INFO)) }
    exitProcess(3)
  }
}

/** Main class of the JVM that [ServerTest.writesQueuedLogsWhenTaskExitsJvm] starts. The class serves [ExitingTask] as task "a" of Dag "c". */
object ExitingTaskProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    Server.create(args).serve(Bundle(listOf(DagDef("c").addTask(TaskDef("a", ExitingTask::class.java)))))
  }
}

/**
 * Starts a daemon thread that logs large lines until the JVM exits, so that the sender thread is usually in the middle of writing one
 * of these lines when the log connection closes.
 */
private fun startBackgroundLogger() {
  val padding = "x".repeat(256 * 1024)
  thread(isDaemon = true) {
    var count = 0
    while (true) LogSender.send(LogMessage("tick ${count++}", mapOf("padding" to padding), "probe", Level.INFO))
  }
}

/** Reads [input] to the end and returns the first part of each line that is not a whole message. */
private fun readCutLines(input: InputStream): List<String> =
  input
    .bufferedReader()
    .lineSequence()
    .filter { line -> runCatching { Json.parseToJsonElement(line).jsonObject }.isFailure }
    .map { it.take(80) }
    .toList()

/** Starts a thread that keeps logging, and returns while the thread is still logging. */
class BackgroundLoggingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    startBackgroundLogger()
    Thread.sleep(200)
  }
}

/** Starts a thread that keeps logging, and then makes the JVM exit with code 3 while the thread is still logging. */
class ExitingBackgroundLoggingTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) {
    startBackgroundLogger()
    // This hook keeps the JVM running for a moment after the SDK's hook closes the log connection. If the
    // sender thread writes a line after the close, that write then has time to fail.
    Runtime.getRuntime().addShutdownHook(Thread { Thread.sleep(500) })
    Thread.sleep(200)
    exitProcess(3)
  }
}

/** Main class of the JVM that [ServerTest.stopsWritingWhenTheServerClosesTheLogConnection] starts. The class serves [BackgroundLoggingTask]. */
object BackgroundLoggingProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    Server.create(args).serve(Bundle(listOf(DagDef("c").addTask(TaskDef("a", BackgroundLoggingTask::class.java)))))
  }
}

/** Main class of the JVM that [ServerTest.stopsWritingWhenTheJvmExits] starts. The class serves [ExitingBackgroundLoggingTask]. */
object ExitingBackgroundLoggingProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    Server.create(args).serve(Bundle(listOf(DagDef("c").addTask(TaskDef("a", ExitingBackgroundLoggingTask::class.java)))))
  }
}

private const val QUEUED_LINES = 64
private val QUEUED_EVENT = "x".repeat(512 * 1024)

/** Reads [input] until it ends, and counts the lines that are whole messages with [QUEUED_EVENT]. */
private fun countQueuedLines(input: InputStream): Int =
  input.bufferedReader().lineSequence().count { line ->
    runCatching {
      Json
        .parseToJsonElement(line)
        .jsonObject["event"]!!
        .jsonPrimitive.content == QUEUED_EVENT
    }.getOrDefault(false)
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

  private fun errorResponseFrame(id: Int): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(3)
      packer.packInt(id)
      packer.packNil()
      packer.packMapHeader(3)
      packer.packString("type").packString("ErrorResponse")
      packer.packString("error").packString("API_SERVER_ERROR")
      packer.packString("detail").packMapHeader(1)
      packer.packString("status_code").packInt(503)
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
  @DisplayName("Should give a task its reply while queued logs wait to be written, and write all of them before the JVM exits")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun taskGetsReplyWhileQueuedLogsAreWritten() =
    withProbeJvm(QueuedLogsProbe::class.java) { comm, logs, process ->
      val toServer = DataOutputStream(comm.getOutputStream())
      val fromServer = DataInputStream(comm.getInputStream())
      // The test reads no logs until the task has reported its result. The queued messages do not fit in
      // the socket's buffers, so they are still waiting to be written while the task runs.
      toServer.writeFrame(hexToBytes(STARTUP_HEX))

      val request = fromServer.readFrame()
      Assertions.assertInstanceOf(GetVariable::class.java, request.body)
      toServer.writeFrame(variableResultFrame(request.id))
      val result = fromServer.readFrame()
      Assertions.assertInstanceOf(SucceedTask::class.java, result.body, "the task should get its reply and succeed")
      toServer.writeFrame(ackFrame(result.id))
      val queued = countQueuedLines(logs.getInputStream())
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the JVM should exit after it reports the result")
      Assertions.assertEquals(0, process.exitValue())
      Assertions.assertEquals(QUEUED_LINES, queued, "every queued message should be written before the log connection closes")
    }

  @Test
  @DisplayName("Should let the JVM exit on SIGTERM while nothing reads the queued logs, and say why lines were lost")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun exitsOnSigtermWhileNothingReadsTheLogs() {
    val output = File.createTempFile("probe", ".out").apply { deleteOnExit() }
    withProbeJvm(UnreadLogsProbe::class.java, output) { comm, logs, process ->
      // The first line shows that the SDK has handed the queued messages to the sender thread. The SDK adds its
      // shutdown hook right after it hands those messages over, on the same thread that later runs the task.
      // So the hook is in place before the task starts.
      logs.getInputStream().bufferedReader().readLine()
      DataOutputStream(comm.getOutputStream()).writeFrame(hexToBytes(STARTUP_HEX))
      Assertions.assertInstanceOf(GetVariable::class.java, DataInputStream(comm.getInputStream()).readFrame().body)

      // The test reads no more logs and never answers the task's request.
      val signalled = System.nanoTime()
      process.destroy()
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the JVM should exit although the queued logs cannot be written")
      val elapsed = Duration.ofNanos(System.nanoTime() - signalled)
      Assertions.assertTrue(elapsed < Duration.ofSeconds(10)) { "the JVM took $elapsed to exit" }
      Assertions.assertEquals(143, process.exitValue(), "the JVM should exit with the exit code that it uses on SIGTERM")
      val reports = output.readLines().filter { it.contains("Cannot send a log line to the supervisor") }
      Assertions.assertTrue(reports.any { it.contains("no log line was written for 2000 ms") }) { "reports: $reports" }
    }
  }

  @Test
  @DisplayName("Should wait at most the log flush timeout for the log socket to close")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun stopsWaitingForLogSocketToClose() {
    val saved = logFlushTimeout
    logFlushTimeout = Duration.ofSeconds(1)
    try {
      LogSender.reset()
      var closeCalled = false
      val started = System.nanoTime()
      // The job never completes. Closing a socket that nothing reads also never completes, because the socket cannot
      // send the rest of its buffer.
      sendQueuedLogs({ closeCalled = true }, Job())
      val elapsed = Duration.ofNanos(System.nanoTime() - started)

      Assertions.assertTrue(closeCalled, "the hook should close the socket")
      Assertions.assertTrue(elapsed >= Duration.ofSeconds(1) && elapsed < Duration.ofSeconds(5)) { "the hook took $elapsed" }
    } finally {
      logFlushTimeout = saved
    }
  }

  @Test
  @DisplayName("Should not wait for the log socket to close once the shutdown hook has stopped waiting for the queued lines")
  @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  fun skipsSocketWaitAfterGivingUpOnQueuedLines() {
    val saved = logFlushTimeout
    val savedErrors = LogSender.errors
    logFlushTimeout = Duration.ofSeconds(2)
    LogSender.errors = PrintStream(OutputStream.nullOutputStream())
    val channel = ByteChannel(autoFlush = true)
    try {
      LogSender.messages.clear()
      LogSender.configure(channel)
      // Nothing reads the channel, so the sender thread cannot finish this line.
      LogSender.send(LogMessage("x".repeat(2 * 1024 * 1024), emptyMap(), "test", Level.INFO))
      var closeCalled = false
      val started = System.nanoTime()
      sendQueuedLogs({ closeCalled = true }, Job())
      val elapsed = Duration.ofNanos(System.nanoTime() - started)

      Assertions.assertTrue(closeCalled, "the hook should still close the socket")
      // The hook gives up on the line after 2 seconds. Waiting for the socket to close would take 2 more seconds.
      Assertions.assertTrue(elapsed < Duration.ofMillis(3500)) { "the hook took $elapsed" }
    } finally {
      channel.cancel(null)
      LogSender.flush(Duration.ofSeconds(10))
      LogSender.errors = savedErrors
      logFlushTimeout = saved
    }
  }

  @Test
  @DisplayName("Should write the queued logs before the JVM exits when the task calls System.exit")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun writesQueuedLogsWhenTaskExitsJvm() =
    withProbeJvm(ExitingTaskProbe::class.java) { comm, logs, process ->
      DataOutputStream(comm.getOutputStream()).writeFrame(hexToBytes(STARTUP_HEX))

      val queued = countQueuedLines(logs.getInputStream())
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the task should make the JVM exit")
      Assertions.assertEquals(3, process.exitValue(), "the JVM should exit with the task's code")
      Assertions.assertEquals(QUEUED_LINES, queued, "every message the task logged should be written before the JVM exits")
    }

  @Test
  @DisplayName("Should write the queued logs before the log connection closes when the supervisor rejects the task's result")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun writesQueuedLogsWhenReportingFails() =
    withProbeJvm(QueuedLogsProbe::class.java) { comm, logs, process ->
      val toServer = DataOutputStream(comm.getOutputStream())
      val fromServer = DataInputStream(comm.getInputStream())
      // The test reads no logs until it has rejected the result, so most queued messages are still waiting
      // to be written when the server stops serving.
      toServer.writeFrame(hexToBytes(STARTUP_HEX))

      val request = fromServer.readFrame()
      toServer.writeFrame(variableResultFrame(request.id))
      val result = fromServer.readFrame()
      toServer.writeFrame(errorResponseFrame(result.id))
      val queued = countQueuedLines(logs.getInputStream())
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the JVM should exit after the server stops serving")
      Assertions.assertEquals(1, process.exitValue(), "the rejected result should fail the JVM")
      Assertions.assertEquals(QUEUED_LINES, queued, "every queued message should be written before the log connection closes")
    }

  @Test
  @DisplayName("Should stop writing lines that another thread logs once the server starts closing the log connection")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun stopsWritingWhenTheServerClosesTheLogConnection() {
    val output = File.createTempFile("probe", ".out").apply { deleteOnExit() }
    withProbeJvm(BackgroundLoggingProbe::class.java, output) { comm, logs, process ->
      val toServer = DataOutputStream(comm.getOutputStream())
      val fromServer = DataInputStream(comm.getInputStream())
      toServer.writeFrame(hexToBytes(STARTUP_HEX))
      var cut = emptyList<String>()
      val reader = thread { cut = readCutLines(logs.getInputStream()) }

      val result = fromServer.readFrame()
      Assertions.assertInstanceOf(SucceedTask::class.java, result.body)
      toServer.writeFrame(ackFrame(result.id))
      reader.join()
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the JVM should exit after it reports the result")
      Assertions.assertEquals(0, process.exitValue())
      assertWholeLinesWithoutWriteErrors(cut, output)
    }
  }

  @Test
  @DisplayName("Should stop writing lines that another thread logs once the JVM starts exiting")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  fun stopsWritingWhenTheJvmExits() {
    val output = File.createTempFile("probe", ".out").apply { deleteOnExit() }
    withProbeJvm(ExitingBackgroundLoggingProbe::class.java, output) { comm, logs, process ->
      DataOutputStream(comm.getOutputStream()).writeFrame(hexToBytes(STARTUP_HEX))

      val cut = readCutLines(logs.getInputStream())
      Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the task should make the JVM exit")
      Assertions.assertEquals(3, process.exitValue(), "the JVM should exit with the task's code")
      assertWholeLinesWithoutWriteErrors(cut, output)
    }
  }

  /**
   * Checks that [cut] is empty and that [output] reports no failed write. [cut] holds the lines that were not
   * whole messages, and [output] holds what the started JVM printed. If a line is written while the SDK closes
   * the log connection, that line can be cut and its write can fail.
   */
  private fun assertWholeLinesWithoutWriteErrors(
    cut: List<String>,
    output: File,
  ) {
    Assertions.assertEquals(emptyList<String>(), cut, "every line should be a whole message")
    val errors = output.readLines().filter { it.contains("Cannot send a log line to the supervisor") }
    Assertions.assertEquals(emptyList<String>(), errors.take(3), "no write should fail (${errors.size} failed)")
  }

  /**
   * Starts a JVM that runs [probe] with a comm connection and a logs connection, and runs [block] with both connections and the JVM.
   * What the started JVM prints goes to [output] if [output] is given, and otherwise to the output of the JVM that runs the test.
   */
  private fun withProbeJvm(
    probe: Class<*>,
    output: File? = null,
    block: (comm: Socket, logs: Socket, process: Process) -> Unit,
  ) {
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
            probe.name,
            "--comm",
            "${loopback.hostAddress}:${commListener.localPort}",
            "--logs",
            "${loopback.hostAddress}:${logsListener.localPort}",
          ).redirectErrorStream(true)
            .redirectOutput(output?.let { ProcessBuilder.Redirect.to(it) } ?: ProcessBuilder.Redirect.INHERIT)
            .start()
        try {
          commListener.accept().use { comm ->
            logsListener.accept().use { logs ->
              comm.soTimeout = 30_000
              logs.soTimeout = 30_000
              block(comm, logs, process)
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
   * [whileRunning], reads the result, and calls [beforeAck] before it acknowledges the result.
   */
  private fun dispatchTask(
    taskClass: Class<out Task>,
    whileRunning: () -> Unit = {},
    beforeAck: () -> Unit = {},
    dag: DagDef = DagDef("c").addTask(TaskDef("a", taskClass)),
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
          toServer.writeFrame(ackFrame(result.id))
        }
      }

    try {
      runBlocking { server.dispatchTask(Bundle(listOf(dag)), comm) }
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
  @DisplayName("Should let a stoppable task see SIGTERM before it first checks, and leave a second SIGTERM to the JVM")
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
  @DisplayName("Should hold the first SIGTERM until the result is reported, without calling onKill for a task that has returned")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalWhileReportingIsHeld() =
    withSignalRecorder("TERM") { recorded ->
      val result =
        dispatchTask(
          ReturningTask::class.java,
          beforeAck = {
            // The runtime is still waiting for this acknowledgement, so it has not finished reporting yet.
            Signal.raise(Signal("TERM"))
            awaitSignalLogs(1)
          },
        )

      Assertions.assertInstanceOf(SucceedTask::class.java, result.body)
      Assertions.assertEquals(listOf("Asking the task to stop"), signalLogs())
      Assertions.assertEquals(emptyList<String>(), taskEvents.toList(), "the task has returned, so there is nothing to stop")
      Assertions.assertNull(recorded.poll(), "SIGTERM should reach the runtime until it has reported the result")
    }

  @Test
  @DisplayName("Should give SIGTERM back to the previous handler after the result is reported")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalAfterReportGoesToTheJvm() =
    withSignalRecorder("TERM") { recorded ->
      dispatchTask(ReturningTask::class.java)

      Signal.raise(Signal("TERM"))

      Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "the SDK should stop catching signals")
      Assertions.assertEquals(emptyList<String>(), signalLogs())
    }

  @Test
  @DisplayName("Should leave SIGTERM to the JVM for a plain task")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalGoesToTheJvmForPlainTask() =
    withSignalRecorder("TERM") { recorded ->
      val handedBack = LinkedBlockingQueue<String>()
      dispatchTask(
        PlainBlockedTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
          handedBack.put(recorded.poll(10, TimeUnit.SECONDS) ?: "nothing")
          releaseTask.put("done")
        },
      )

      Assertions.assertEquals("SIGTERM", handedBack.take(), "SIGTERM should reach the JVM's handler while the task runs")
      Assertions.assertEquals(emptyList<String>(), signalLogs(), "the SDK should not see a signal it does not catch")
    }

  @Test
  @DisplayName("Should fail a plain task that checks for a stop request, because it can never see one")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun failsPlainTaskThatChecksForStopRequest() {
    for (taskClass in listOf(PlainCheckingTask::class.java, PlainWaitingTask::class.java)) {
      val result = dispatchTask(taskClass)

      Assertions.assertEquals(TaskState.State.FAILED, (result.body as TaskState).state, taskClass.simpleName)
      val error = LogSender.messages.single { it.event == "Error executing task" }.arguments["error"]
      Assertions.assertInstanceOf(IllegalStateException::class.java, error, taskClass.simpleName)
      Assertions.assertEquals(NOT_STOPPABLE_TASK_ERROR, (error as Throwable).message, taskClass.simpleName)
    }
  }

  @Test
  @DisplayName("Should call onKill while the task is blocked, with the task's context, after making the request visible")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun onKillStopsBlockedTask() =
    withSignalRecorder("TERM") { recorded ->
      val result =
        dispatchTask(
          BlockedTask::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
        )

      Assertions.assertEquals(
        listOf(
          "onKill got the context of execute: true",
          "onKill got the class loader of execute: true",
          "onKill saw isCancelled() return true",
          "execute returned",
        ),
        taskEvents.toList(),
      )
      Assertions.assertInstanceOf(SucceedTask::class.java, result.body, "the task returned, so its success is reported")
      Assertions.assertEquals(listOf("Asking the task to stop"), signalLogs())
      Assertions.assertNull(recorded.poll(), "the SDK should keep the SIGTERM for the task")
      Assertions.assertTrue(
        LogSender.messages.none { it.event == INTERRUPT_CLEARED_EVENT },
        "the task left its thread's interrupt flag clear",
      )
    }

  @Test
  @DisplayName("Should report the result only after onKill returns")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun reportsResultAfterOnKillReturns() =
    withSignalRecorder("TERM") { _ ->
      dispatchTask(
        ReturnsDuringOnKillTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
        },
        beforeAck = { taskEvents.put("result reported") },
      )

      Assertions.assertEquals(listOf("execute returned", "onKill returned", "result reported"), taskEvents.toList())
    }

  @Test
  @DisplayName("Should report a failure, after onKill returns, when onKill cancels a future the task waits for")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun reportsFailureWhenOnKillCancelsFuture() =
    withSignalRecorder("TERM") { _ ->
      val result =
        dispatchTask(
          CancelledFutureTask::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
          beforeAck = { taskEvents.put("result reported") },
        )

      Assertions.assertEquals(TaskState.State.FAILED, (result.body as TaskState).state)
      Assertions.assertEquals(listOf("onKill returned", "result reported"), taskEvents.toList())
    }

  @Test
  @DisplayName("Should report the result and clear the flag when the task leaves its thread interrupted")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun reportsResultWhenTaskLeavesThreadInterrupted() =
    withSignalRecorder("TERM") { _ ->
      val result =
        dispatchTask(
          InterruptedTask::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
        )

      Assertions.assertEquals(TaskState.State.FAILED, (result.body as TaskState).state)
      Assertions.assertFalse(Thread.interrupted(), "the SDK should clear the flag before it waits for the acknowledgement")
      Assertions.assertEquals(
        1,
        LogSender.messages.count { it.event == INTERRUPT_CLEARED_EVENT },
        "the SDK should log that it cleared the interrupt flag",
      )
    }

  @Test
  @DisplayName("Should call onKill for a condition that is blocked in decide")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun onKillStopsBlockedCondition() =
    withSignalRecorder("TERM") { recorded ->
      val dag = DagDef("c")
      dag.If("a", BlockedCondition::class.java).Then(dag.task<Unit>("load", ServerNoopTask::class.java))

      val result =
        dispatchTask(
          BlockedCondition::class.java,
          whileRunning = {
            awaitTaskEvent("started")
            Signal.raise(Signal("TERM"))
          },
          dag = dag,
        )

      Assertions.assertEquals(TaskState.State.FAILED, (result.body as TaskState).state)
      Assertions.assertEquals(listOf("onKill ran"), taskEvents.toList())
      Assertions.assertNull(recorded.poll(), "the SDK should keep the SIGTERM for the task")
    }

  @Test
  @DisplayName("Should leave a second SIGTERM to the JVM while onKill is still running")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun secondSignalGoesToTheJvmWhileOnKillRuns() =
    withSignalRecorder("TERM") { recorded ->
      val handedBack = LinkedBlockingQueue<String>()
      dispatchTask(
        HangingOnKillTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
          awaitTaskEvent("onKill started")
          Signal.raise(Signal("TERM"))
          handedBack.put(recorded.poll(10, TimeUnit.SECONDS) ?: "nothing")
          releaseHook.put("done")
          releaseTask.put("done")
        },
      )

      Assertions.assertEquals("SIGTERM", handedBack.take(), "a hanging onKill should not hold back the second SIGTERM")
      Assertions.assertEquals(listOf("Asking the task to stop", "Letting the JVM handle the signal"), signalLogs())
      Assertions.assertEquals(listOf("onKill returned"), taskEvents.toList(), "onKill should run once")
    }

  @Test
  @DisplayName("Should log a failing onKill and leave the signal to the JVM")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun failingOnKillLeavesSignalToTheJvm() =
    withSignalRecorder("TERM") { recorded ->
      val handedBack = LinkedBlockingQueue<String>()
      dispatchTask(
        FailingOnKillTask::class.java,
        whileRunning = {
          awaitTaskEvent("started")
          Signal.raise(Signal("TERM"))
          handedBack.put(recorded.poll(10, TimeUnit.SECONDS) ?: "nothing")
          releaseTask.put("done")
        },
      )

      Assertions.assertEquals("SIGTERM", handedBack.take(), "the JVM's handler should get the signal when onKill fails")
      val failure = LogSender.messages.single { it.event == "Task's onKill failed, so letting the JVM handle the signal" }
      Assertions.assertEquals("SIGTERM", failure.arguments["signal"])
      Assertions.assertEquals("the job cannot be cancelled", (failure.arguments["error"] as Throwable).message)
    }

  @Test
  @DisplayName("Should leave a signal to the JVM until the SDK starts the task, and not call onKill for it")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun signalBeforeTaskStartsGoesToTheJvm() =
    withSignalRecorder("TERM") { recorded ->
      jvmSignals = recorded
      try {
        dispatchTask(SignalInConstructorTask::class.java)
      } finally {
        jvmSignals = null
      }

      Assertions.assertEquals(listOf("the JVM's handler got SIGTERM"), taskEvents.toList())
      Assertions.assertEquals(emptyList<String>(), signalLogs())
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
    const val NOT_STOPPABLE_TASK_ERROR =
      "Only a StoppableTask can see a stop request. Implement StoppableTask, or give an annotated task an @Airflow.OnKill method"
    const val INTERRUPT_CLEARED_EVENT = "Cleared the interrupt flag that the task left on its thread, so the SDK can report the result"

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
