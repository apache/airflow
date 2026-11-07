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

import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeString
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import java.io.FileDescriptor
import java.io.FileOutputStream
import java.io.IOException
import java.io.PrintStream
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.reflect.KClass

// Adapted from Python logging.
enum class Level(
  val value: Short,
) {
  CRITICAL(50),
  ERROR(40),
  WARNING(30),
  INFO(20),
  DEBUG(10),
  NOTSET(0),
  ;

  internal companion object {
    val accepted = entries.associateBy { it.name }

    fun parse(s: String?): Level? = s?.let { Level.accepted[it.trim().uppercase()] }
  }
}

/**
 * Parser for the namespace levels configuration.
 *
 * The value is a series of `<logger>=<level>` pairs separated by whitespaces and/or commas.
 * Each `<level>` must name one of [Level]s (case-insensitive). When the same logger appears
 * more than once, the last value wins.
 */
internal object NamespaceLevels {
  const val ENV_VAR = "AIRFLOW__LOGGING__NAMESPACE_LEVELS"
  private const val LOGGER_NAME = "org.apache.airflow.sdk.execution.NamespaceLevels"

  /**
   * Parse [raw] into per-logger [Level] overrides.
   *
   * Invalid entries are skipped — the affected logger then falls back to the global level — and
   * reported once at [Level.ERROR], so a misconfiguration never takes down logging nor discards the
   * valid overrides. A `null`, empty, or whitespace/comma-only value yields no overrides.
   * Surrounding and repeated separators are ignored.
   */
  fun parse(raw: String?): Map<String, Level> {
    if (raw.isNullOrBlank()) return emptyMap()

    val levels = mutableMapOf<String, Level>()
    val errors = mutableListOf<String>()
    raw
      .split(Regex("""[\s,]+"""))
      .filter { it.isNotEmpty() }
      .forEach { entry -> parseEntry(entry, levels)?.let { errors += it } }

    if (errors.isNotEmpty()) {
      LogSender.send(
        LogMessage(
          event = "Ignoring invalid $ENV_VAR entries: ${errors.joinToString("; ")}",
          arguments = emptyMap(),
          logger = LOGGER_NAME,
          level = Level.ERROR,
        ),
      )
    }
    return levels
  }

  /** Parse a single [entry] into [levels], or return a description of why it was skipped. */
  private fun parseEntry(
    entry: String,
    levels: MutableMap<String, Level>,
  ): String? {
    val separator = entry.indexOf('=')
    if (separator < 0) {
      return "malformed entry \"$entry\", expected \"<logger>=<level>\""
    }

    val logger = entry.substring(0, separator).trim()
    if (logger.isEmpty()) {
      return "malformed entry \"$entry\", logger name is empty"
    }

    val levelName = entry.substring(separator + 1).trim()
    val level =
      Level.parse(levelName)
        ?: return "invalid level \"$levelName\" for logger \"$logger\", " +
          "expected one of: ${Level.entries.joinToString(", ") { it.name }}"

    levels[logger] = level
    return null
  }
}

/**
 * Public entry point into Airflow's log pipeline.
 *
 * This is useful for Java-side logging providers such as [java.util.logging]
 * and SLF4J to integrate logs they receive into Airflow.
 *
 * Not intended for use by task code.
 */
object Log {
  private const val LOGGING_LEVEL_ENV = "AIRFLOW__LOGGING__LOGGING_LEVEL"

  internal var globalThreshold = Level.parse(System.getenv(LOGGING_LEVEL_ENV)) ?: Level.INFO
  internal var namedThresholds = NamespaceLevels.parse(System.getenv(NamespaceLevels.ENV_VAR))

  /**
   * Whether a [level] message from the logger called [name] should be emitted.
   *
   * Thresholds cascade down the dotted-name hierarchy, mirroring Java logging
   * convention. A logger inherits the level of its nearest configured
   * ancestor, so with `foo=WARNING` configured, loggers named e.g. `foo.bar`
   * also resolves to `WARNING` without additional configuration. A logger with
   * no configured ancestor falls back to [globalThreshold].
   */
  fun isEnabledForLevel(
    level: Level,
    name: String?,
  ): Boolean {
    var threshold = name
    while (threshold != null) {
      namedThresholds[threshold]?.let { return level.value >= it.value }
      threshold = threshold.substringBeforeLast('.', missingDelimiterValue = "").ifEmpty { null }
    }
    return level.value >= globalThreshold.value
  }

  fun send(
    level: Level,
    logger: String,
    event: String,
    arguments: Map<String, Any?> = emptyMap(),
  ) {
    if (!isEnabledForLevel(level, logger)) return
    LogSender.send(LogMessage(event, arguments, logger, level))
  }

  fun send(
    level: Level,
    logger: String,
    event: String,
    buildArguments: MutableMap<String, Any?>.() -> Unit,
  ) = send(level, logger, event, buildMap(buildArguments))
}

internal data class LogMessage(
  val event: String,
  val arguments: Map<String, Any?>,
  val logger: String,
  val level: Level,
  val timestamp: Instant = Instant.now(),
)

/**
 * Logger used by task scaffolding.
 *
 * This is a thin wrapper around [LogSender] that our own code can
 * use instead of needing to go through a "real" logging provider.
 *
 * A logger made with [waitsForRoom] set to false never waits for room in [LogSender]. The SDK uses such a
 * logger on a thread that must keep going while nothing reads the logs.
 */
internal class Logger(
  val name: String,
  private val waitsForRoom: Boolean = true,
) {
  constructor(cls: KClass<*>, waitsForRoom: Boolean = true) : this(cls.java.typeName, waitsForRoom)

  fun debug(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.DEBUG, message, arguments)

  fun info(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.INFO, message, arguments)

  fun warning(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.WARNING, message, arguments)

  fun error(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.ERROR, message, arguments)

  private fun log(
    level: Level,
    event: String,
    arguments: Map<String, Any>,
  ) {
    if (!Log.isEnabledForLevel(level, name)) return
    LogSender.send(LogMessage(event, arguments, name, level), waitsForRoom)
  }
}

// Several threads log: the task's own thread, the thread that handles a stop signal, and the thread
// that reads replies from the supervisor. The lock keeps send from adding a message to messages after
// configure has passed the messages in that queue to the sender thread. The lock also keeps a new message
// from being written before the messages that were queued earlier.
internal object LogSender {
  /** The number of characters that the queued lines can hold before a caller has to wait for room. */
  internal const val PENDING_CHARS = 8 * 1024 * 1024

  private var writer: ByteWriteChannel? = null

  // This field holds the channel that configure got last. Unlike writer, this field keeps the channel after
  // detachAndFlush, so a later detachAndFlush call can still cancel the channel while an earlier call is stuck.
  private var configured: ByteWriteChannel? = null
  internal val messages: ConcurrentLinkedDeque<LogMessage> = ConcurrentLinkedDeque()

  // Only the sender thread writes to the channel, so a caller never waits in runBlocking. While
  // runBlocking waits, it also runs other coroutines that are ready on the calling thread. On the thread
  // that Server.serve blocks, one of those coroutines runs the task. If a caller waited in runBlocking on
  // that thread, the task would run while the caller holds this object's lock. The task would then hang if
  // it waited for another thread that logs, such as the thread that reads replies from the supervisor.
  private val pending = LinkedBlockingQueue<() -> Unit>()

  // A queued line holds one permit for each of its characters until the sender thread has written the line.
  // The semaphore is fair, so that short lines cannot keep taking the room that a long line waits for.
  private val room = Semaphore(PENDING_CHARS, true)

  // The sender thread reports a failed write on the stderr of the process. The supervisor puts that stderr in the
  // task log. Reporting on System.err would not be safe. A logging bridge can lead System.err back into LogSender,
  // and a caller can hold the lock of System.err while it waits for room.
  @Volatile
  internal var errors = PrintStream(FileOutputStream(FileDescriptor.err), true)

  // This field holds the channel of the last write that failed. Every later write to that channel fails the same way,
  // so the sender thread reports only the first failed write to each channel. Only the sender thread uses this field.
  private var failedChannel: ByteWriteChannel? = null

  // This list holds the reports of failed writes that the sender thread has not printed yet. A report can block while
  // nothing reads the stderr of the process. If the sender thread printed a report as soon as it failed to write a
  // line, the lines queued after that line would keep their room while the report blocks. So the sender thread prints
  // the reports only when it has no line left to write, or when it runs the task that flush queued. Only the sender
  // thread uses this list.
  private val reports = ArrayList<String>()

  // This field holds the time when the sender thread last finished with a line, whether it wrote the line or failed
  // to write it. flush counts its idle timeout from this time.
  @Volatile
  private var finishedAt = System.nanoTime()

  private val sender =
    thread(name = "Airflow log sender", isDaemon = true) {
      while (true) {
        try {
          val task =
            pending.poll() ?: run {
              printReports()
              pending.take()
            }
          task.invoke()
        } catch (e: Throwable) {
          // If the sender thread ended, flush would wait forever, and so would every caller that waits for room.
        }
      }
    }

  @Synchronized
  fun configure(channel: ByteWriteChannel) {
    writer = channel
    configured = channel
    if (!channel.isClosedForWrite) {
      while (messages.isNotEmpty()) {
        // Waiting for room would save no memory here, because the earlier messages are already in memory.
        // Waiting would also block the thread that serves the task.
        queueLine(channel, buildLine(messages.removeFirst()), chars = 0)
      }
    }
  }

  fun send(
    message: LogMessage,
    waitForRoom: Boolean = true,
  ) {
    synchronized(this) {
      // A message that stays in messages takes no room, so it does not wait for room.
      if (!isQueuing()) {
        messages.addLast(message)
        return
      }
    }
    // Build the line now, so that it shows the arguments as they were when the caller logged them.
    val line = buildLine(message)
    // Only the sender thread gives back the room of a queued line, so the sender thread does not wait for room
    // when it logs, for example through a logging bridge. A line longer than the limit waits until the sender
    // thread has written every other queued line.
    val chars = if (waitForRoom && Thread.currentThread() !== sender) minOf(line.length, PENDING_CHARS) else 0
    // Wait for room before taking the lock. Otherwise, if the sender thread logged while a caller held the lock
    // and waited for room, each of the two threads would wait for the other. Skip the semaphore when the line
    // needs no room. A fair semaphore makes even a request for zero permits wait while other threads wait for
    // room. Code that restores a thread's interrupt status often logs next, so keep waiting if the calling
    // thread is interrupted. acquireUninterruptibly also keeps the calling thread's interrupt status.
    if (chars > 0) room.acquireUninterruptibly(chars)
    synchronized(this) {
      val channel = writer
      if (channel == null || channel.isClosedForWrite) {
        // Queuing stopped while this caller waited for room.
        messages.addLast(message)
        room.release(chars)
      } else {
        queueLine(channel, line, chars)
      }
    }
  }

  /**
   * Waits until the sender thread has finished with the lines queued so far and has printed the reports of failed
   * writes, and returns true. If [idleTimeout] is not null, this function stops waiting once the sender thread has
   * not finished with a line for that long, and then returns false. This function keeps waiting if the calling
   * thread is interrupted.
   */
  fun flush(idleTimeout: Duration? = null): Boolean {
    val written = CompletableFuture<Unit>()
    pending.add {
      try {
        printReports()
      } finally {
        written.complete(Unit)
      }
    }
    if (idleTimeout == null) {
      written.join()
      return true
    }
    val started = System.nanoTime()
    while (!written.isDone) {
      // If the sender thread last finished with a line before this call, the idle time counts from the start of this
      // call. So flush gives up no earlier than idleTimeout after this call starts.
      val last = finishedAt.let { if (it - started > 0) it else started }
      val left = idleTimeout.toNanos() - (System.nanoTime() - last)
      if (left <= 0) return false
      written.copy().completeOnTimeout(Unit, left, TimeUnit.NANOSECONDS).join()
    }
    return true
  }

  /**
   * Stops queuing lines for the channel and waits as [flush] does. This function returns true once the sender thread
   * has finished with the queued lines. If [flush] stops waiting because of [idleTimeout], this function cancels the
   * channel and returns false. A message that is logged after this function stops queuing goes to [messages]. A
   * message that is still waiting for room at that point also goes to [messages], once it gets room. Once the queued
   * lines are written, closing the channel cannot cut off a line that the sender thread is still writing.
   */
  fun detachAndFlush(idleTimeout: Duration? = null): Boolean {
    val channel =
      synchronized(this) {
        writer = null
        configured
      }
    if (flush(idleTimeout)) return true
    // If the channel were not cancelled, the line that the sender thread cannot write would keep its room, and so
    // would every line after it. A caller that waits for room, such as another shutdown hook that logs, would then
    // wait forever.
    channel?.cancel(IOException("Stopped waiting after no log line was written for ${idleTimeout?.toMillis()} ms"))
    // The remaining lines now fail at once. Give the sender thread a moment to report why, because the JVM can exit
    // as soon as this function returns.
    flush(Duration.ofSeconds(1))
    return false
  }

  internal fun reset() {
    writer = null
    configured = null
    messages.clear()
  }

  private fun isQueuing(): Boolean = writer?.isClosedForWrite == false

  private fun buildLine(message: LogMessage): String {
    val map = message.arguments.toMutableMap()
    map["event"] = message.event
    map["level"] = message.level.name.lowercase()
    map["logger"] = message.logger
    map["timestamp"] = message.timestamp.toString()
    return "${map.toJsonElement()}\n"
  }

  private fun queueLine(
    writer: ByteWriteChannel,
    line: String,
    chars: Int,
  ) {
    pending.add {
      val failure =
        try {
          runBlocking { writer.writeString(line) }
          null
        } catch (e: Throwable) {
          e
        } finally {
          finishedAt = System.nanoTime()
          room.release(chars)
        }
      if (failure != null && failedChannel !== writer) {
        failedChannel = writer
        reports.add("Cannot send a log line to the supervisor: $failure")
      }
    }
  }

  private fun printReports() {
    while (reports.isNotEmpty()) errors.println(reports.removeAt(0))
  }
}

private fun Any?.toJsonElement(): JsonElement =
  when (this) {
    is JsonElement -> this
    is Map<*, *> ->
      buildJsonObject {
        forEach { (k, v) -> put(k.toString(), v.toJsonElement()) }
      }
    is Iterable<*> -> buildJsonArray { forEach { add(it.toJsonElement()) } }
    is Number -> JsonPrimitive(this)
    is String -> JsonPrimitive(this)
    null -> JsonNull
    else -> JsonPrimitive(toString())
  }
