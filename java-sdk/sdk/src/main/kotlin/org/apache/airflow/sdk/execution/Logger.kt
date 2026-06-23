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

enum class Level { ERROR, DEBUG, }

internal data class LogMessage(
  val event: String,
  val arguments: Map<String, Any>,
  val logger: Logger,
  val level: Level,
  val timestamp: LocalDateTime = Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault()),
)

internal class Logger(
  cls: KClass<*>,
) {
  val name: String? = cls.java.typeName

  // TODO: Actually implement level filtering.
  @Suppress("UNUSED_PARAMETER")
  fun isEnabledForLevel(level: Level): Boolean = true

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
    LogSender.send(LogMessage(event, arguments, this, level))
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

  private fun sendTo(
    writer: ByteWriteChannel,
    message: LogMessage,
  ) {
    val map = message.arguments.toMutableMap()
    map["event"] = message.event
    map["level"] = message.level.name.lowercase()
    map["logger"] = message.logger.name ?: "(java)"
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
