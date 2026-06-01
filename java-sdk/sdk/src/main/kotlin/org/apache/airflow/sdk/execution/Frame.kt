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

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.airflow.sdk.execution.comm.Discriminator
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream

object Frame {
  private val mapper =
    ObjectMapper().apply {
      configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      registerModule(JavaTimeModule())
      registerModule(TimestampToJavaOffsetDateTimeModule())
      setDateFormat(StdDateFormat().withColonInTimeZone(true))
    }

  fun encodeRequest(
    id: Int,
    body: Any,
  ): ByteArray = encodeFrame(id, body)

  fun decode(bytes: ByteArray): IncomingFrame {
    val unpacker = MessagePack.newDefaultUnpacker(bytes)
    val headerSize = unpacker.unpackArrayHeader()
    check(headerSize >= 1) { "Unexpected Task SDK frame arity $headerSize" }

    val id = unpacker.unpackInt()
    val rawBody = if (headerSize >= 2) unpacker.unpackAny() else null
    val rawError = if (headerSize >= 3) unpacker.unpackAny() else null
    unpacker.close()

    val body = decodeMessage(rawError) ?: decodeMessage(rawBody)
    return IncomingFrame(id, body)
  }

  fun lengthPrefix(length: Int) =
    byteArrayOf(
      (length shr 24).toByte(),
      (length shr 16).toByte(),
      (length shr 8).toByte(),
      length.toByte(),
    )

  fun parseLengthPrefix(prefix: ByteArray): Int {
    check(prefix.size == 4) { "Need 4 prefix bytes" }
    return prefix.fold(0) { acc, byte -> (acc shl 8) or (byte.toInt() and 0xff) }
  }

  private fun encodeFrame(
    id: Int,
    body: Any?,
  ): ByteArray {
    val payload = ByteArrayOutputStream()
    val packer = MessagePack.newDefaultPacker(payload)
    packer.packArrayHeader(2)
    packer.packInt(id)
    packer.packAny(body?.let(::toBody))
    packer.close()
    return payload.toByteArray()
  }

  private fun decodeMessage(raw: Any?): Any? {
    val body = raw as? Map<*, *> ?: return raw
    return mapper.convertValue(body, Discriminator.discriminate(body) ?: return raw)
  }

  @Suppress("UNCHECKED_CAST")
  private fun toBody(value: Any): Map<String, Any?> =
    when (value) {
      is Map<*, *> -> value as Map<String, Any?>
      else -> mapper.convertValue(value, MutableMap::class.java) as MutableMap<String, Any?>
    }
}

private fun Discriminator.discriminate(body: Map<*, *>): Class<*>? {
  val name = body["type"] as? String ?: return null
  return types[name] ?: error("Unsupported supervisor message type: $name")
}
