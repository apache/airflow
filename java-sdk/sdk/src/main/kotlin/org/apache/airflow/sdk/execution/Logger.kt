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
 * With [waitsForRoom] set to false, this logger never waits for room in [LogSender].
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

// This object's lock keeps send, configure and detachAndFlush from losing or reordering lines.
internal object LogSender {
  internal const val PENDING_CHARS = 8 * 1024 * 1024

  private var writer: ByteWriteChannel? = null

  // Kept after detachAndFlush clears writer, so a later call can cancel the channel an earlier one is stuck on.
  private var configured: ByteWriteChannel? = null
  internal val messages: ConcurrentLinkedDeque<LogMessage> = ConcurrentLinkedDeque()

  // Set by detachAndFlush. Nothing writes LogSender.messages after that call, so send prints lines to errors instead.
  private var detached = false

  // Only the sender thread writes, so a caller's runBlocking cannot run the task under this object's lock.
  private val pending = LinkedBlockingQueue<() -> Unit>()

  // One permit per queued character. Fair, so short lines cannot keep taking room a long line waits for.
  private val room = Semaphore(PENDING_CHARS, true)

  // Not System.err, which a logging bridge can route back here and whose lock a waiting caller can hold.
  @Volatile
  internal var errors = PrintStream(FileOutputStream(FileDescriptor.err), true)

  private var failedChannel: ByteWriteChannel? = null

  // Printing a report when a line fails could stall the lines already queued, so wait for an empty queue or a flush.
  private val reports = ArrayList<String>()

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
    detached = false
    if (!channel.isClosedForWrite) {
      while (messages.isNotEmpty()) {
        // Don't wait for room: these messages are already in memory, and waiting would block the serving thread.
        queueLine(channel, buildLine(messages.removeFirst()), chars = 0)
      }
    }
  }

  fun send(
    message: LogMessage,
    waitForRoom: Boolean = true,
  ) {
    val detachedNow =
      synchronized(this) {
        when {
          isQueuing() -> false
          detached -> true
          else -> {
            messages.addLast(message)
            return
          }
        }
      }
    if (detachedNow) {
      printAfterDetach(message)
      return
    }
    // Build the line now, so that it shows the arguments as they were when the caller logged them.
    val line = buildLine(message)
    // Only the sender thread gives back room, so it never waits. A line over the limit asks for all the room.
    val chars = if (waitForRoom && Thread.currentThread() !== sender) minOf(line.length, PENDING_CHARS) else 0
    // Wait before locking, or a logging sender thread can deadlock. A fair semaphore makes acquire(0) wait too.
    if (chars > 0) room.acquireUninterruptibly(chars)
    val detachedWhileWaiting =
      synchronized(this) {
        val channel = writer
        if (channel == null || channel.isClosedForWrite) {
          // Queuing stopped while this caller waited for room.
          room.release(chars)
          if (!detached) messages.addLast(message)
          detached
        } else {
          queueLine(channel, line, chars)
          false
        }
      }
    if (detachedWhileWaiting) printAfterDetach(message, line)
  }

  // The supervisor logs the JVM's stderr at ERROR level, so drop lines below WARNING.
  private fun printAfterDetach(
    message: LogMessage,
    line: String? = null,
  ) {
    if (message.level.value >= Level.WARNING.value) errors.print(line ?: buildLine(message))
  }

  /**
   * Waits for the queued lines and their failure reports, and returns true.
   * Returns false instead once no line finishes for [idleTimeout].
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
      // Count idle time from this call if the last line finished before it.
      val last = finishedAt.let { if (it - started > 0) it else started }
      val left = idleTimeout.toNanos() - (System.nanoTime() - last)
      if (left <= 0) return false
      written.copy().completeOnTimeout(Unit, left, TimeUnit.NANOSECONDS).join()
    }
    return true
  }

  /**
   * Stops queuing lines for the configured channel and waits as [flush] does.
   * If that wait gives up, cancels the channel and returns false.
   */
  fun detachAndFlush(idleTimeout: Duration? = null): Boolean {
    val channel =
      synchronized(this) {
        writer = null
        detached = true
        configured
      }
    if (flush(idleTimeout)) return true
    // Otherwise the stuck line and every line behind it keep their room, and callers waiting for room hang.
    channel?.cancel(IOException("Stopped waiting after no log line was written for ${idleTimeout?.toMillis()} ms"))
    // Queued lines now fail at once. Give the sender thread a moment to report why before the JVM can exit.
    flush(Duration.ofSeconds(1))
    return false
  }

  @Synchronized
  internal fun reset() {
    writer = null
    configured = null
    detached = false
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
