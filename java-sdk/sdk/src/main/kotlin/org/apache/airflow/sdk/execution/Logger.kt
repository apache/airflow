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
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.reflect.KClass
import kotlin.time.Clock

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
  val timestamp: LocalDateTime = Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault()),
)

/**
 * Logger used by task scaffolding.
 *
 * This is a thin wrapper around [LogSender] that our own code can
 * use instead of needing to go through a "real" logging provider.
 */
internal class Logger(
  val name: String,
) {
  constructor(cls: KClass<*>) : this(cls.java.typeName)

  fun debug(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.DEBUG, message, arguments)

  fun error(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.ERROR, message, arguments)

  fun warning(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) = log(Level.WARNING, message, arguments)

  private fun log(
    level: Level,
    event: String,
    arguments: Map<String, Any>,
  ) {
    if (!Log.isEnabledForLevel(level, name)) return
    LogSender.send(LogMessage(event, arguments, name, level))
  }
}

internal object LogSender {
  private var writer: ByteWriteChannel? = null
  internal val messages: ConcurrentLinkedDeque<LogMessage> = ConcurrentLinkedDeque()

  fun configure(channel: ByteWriteChannel) {
    writer = channel
    if (!channel.isClosedForWrite) {
      while (messages.isNotEmpty()) {
        sendTo(channel, messages.removeFirst())
      }
    }
  }

  fun send(message: LogMessage) {
    val channel = writer
    if (channel == null || channel.isClosedForWrite) {
      messages.addLast(message)
    } else {
      sendTo(channel, message)
    }
  }

  private fun sendTo(
    writer: ByteWriteChannel,
    message: LogMessage,
  ) {
    val map = message.arguments.toMutableMap()
    map["event"] = message.event
    map["level"] = message.level.name.lowercase()
    map["logger"] = message.logger
    map["timestamp"] = message.timestamp
    // TODO: Can this be done asynchronously instead?
    runBlocking { writer.writeString("${map.toJsonElement()}\n") }
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
    else -> JsonPrimitive(toString()) // Also correctly handles Kotlinx DateTime.
  }
