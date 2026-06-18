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

// wireName is the level string the Airflow supervisor understands (structlog's
// NAME_TO_LEVEL). It has no TRACE, so TRACE maps to "debug"; a level the
// supervisor does not recognise is dropped silently on the Python side.
// severity mirrors Python's logging numeric values (DEBUG=10 ... ERROR=40) so a
// level can be compared against the configured threshold; TRACE sits below DEBUG.
enum class Level(
  val wireName: String,
  val severity: Int,
) {
  TRACE("debug", 5),
  DEBUG("debug", 10),
  INFO("info", 20),
  WARN("warning", 30),
  ERROR("error", 40),
}

/**
 * Resolves the effective task log level the JVM should emit.
 *
 * The supervisor reconfigures the task logger with `level_override=NOTSET`,
 * delegating threshold filtering to the subprocess, so the Java SDK must drop
 * sub-threshold events itself. The threshold comes from the `airflow.logging.level`
 * system property (an explicit JVM flag wins) and otherwise the
 * `AIRFLOW__LOGGING__LOGGING_LEVEL` env var the subprocess inherits from the
 * supervisor; unset or unrecognised values fall back to INFO.
 */
internal object LogLevel {
  const val LEVEL_PROPERTY = "airflow.logging.level"
  const val LEVEL_ENV = "AIRFLOW__LOGGING__LOGGING_LEVEL"

  fun threshold(): Level = parse(configuredLevel()) ?: Level.INFO

  fun isEnabled(level: Level): Boolean = level.severity >= threshold().severity

  fun parse(name: String?): Level? =
    when (name?.trim()?.uppercase()) {
      "TRACE" -> Level.TRACE
      "DEBUG" -> Level.DEBUG
      "INFO" -> Level.INFO
      "WARN", "WARNING" -> Level.WARN
      // The Java SDK has no CRITICAL/FATAL, so the strictest threshold it can honour is ERROR.
      "ERROR", "CRITICAL", "FATAL" -> Level.ERROR
      else -> null
    }

  private fun configuredLevel(): String? = System.getProperty(LEVEL_PROPERTY) ?: System.getenv(LEVEL_ENV)
}

internal data class LogMessage(
  val event: String,
  val arguments: Map<String, Any>,
  val loggerName: String,
  val level: Level,
  val timestamp: LocalDateTime = Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault()),
)

internal class Logger(
  cls: KClass<*>,
) {
  val name: String? = cls.java.typeName

  fun isEnabledForLevel(level: Level): Boolean = LogLevel.isEnabled(level)

  fun debug(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) {
    log(Level.DEBUG, message, arguments)
  }

  fun error(
    message: String,
    arguments: Map<String, Any> = emptyMap(),
  ) {
    log(Level.ERROR, message, arguments)
  }

  private fun log(
    level: Level,
    event: String,
    arguments: Map<String, Any>,
  ) {
    if (!isEnabledForLevel(level)) return
    LogSender.send(LogMessage(event, arguments, name ?: "(java)", level))
  }
}

internal object LogSender {
  private var writer: ByteWriteChannel? = null
  private val messages: ConcurrentLinkedDeque<LogMessage> = ConcurrentLinkedDeque()

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

  internal fun encode(message: LogMessage): String {
    val map = message.arguments.toMutableMap()
    map["event"] = message.event
    map["level"] = message.level.wireName
    map["logger"] = message.loggerName
    map["timestamp"] = message.timestamp
    return "${map.toJsonElement()}\n"
  }

  private fun sendTo(
    writer: ByteWriteChannel,
    message: LogMessage,
  ) {
    // TODO: Can this be done asynchronously instead?
    runBlocking { writer.writeString(encode(message)) }
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
