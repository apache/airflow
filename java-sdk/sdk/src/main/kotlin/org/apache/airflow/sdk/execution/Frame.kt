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
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.buffer.MessageBuffer
import org.msgpack.core.buffer.MessageBufferInput

object Frame {
  internal const val MAX_FRAME_LENGTH = 0xFFFF_FFFFL

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
  ): List<MessageBuffer> = encodeFrame(id, body)

  fun decode(input: MessageBufferInput): IncomingFrame = MessagePack.newDefaultUnpacker(input).use { decodeFrom(it) }

  private fun decodeFrom(unpacker: MessageUnpacker): IncomingFrame {
    val headerSize = unpacker.unpackArrayHeader()
    check(headerSize >= 1) { "Unexpected Task SDK frame arity $headerSize" }

    val id = unpacker.unpackInt()
    val rawBody = if (headerSize >= 2) unpacker.unpackAny() else null
    val rawError = if (headerSize >= 3) unpacker.unpackAny() else null

    val body = decodeMessage(rawError) ?: decodeMessage(rawBody)
    return IncomingFrame(id, body)
  }

  fun lengthPrefix(length: UInt) =
    byteArrayOf(
      (length shr 24).toByte(),
      (length shr 16).toByte(),
      (length shr 8).toByte(),
      length.toByte(),
    )

  fun payloadLength(buffers: List<MessageBuffer>): UInt {
    val total = buffers.sumOf { it.size().toLong() }
    require(total <= MAX_FRAME_LENGTH) {
      "Frame payload $total bytes exceeds protocol maximum $MAX_FRAME_LENGTH"
    }
    return total.toUInt()
  }

  fun parseLengthPrefix(prefix: ByteArray): UInt {
    check(prefix.size == 4) { "Need 4 prefix bytes" }
    return prefix.fold(0u) { acc, byte -> (acc shl 8) or (byte.toUInt() and 0xffu) }
  }

  private fun encodeFrame(
    id: Int,
    body: Any?,
  ): List<MessageBuffer> {
    val packer = MessagePack.newDefaultBufferPacker()
    packer.packArrayHeader(2)
    packer.packInt(id)
    packer.packAny(body?.let(::toBody))
    packer.close()
    return packer.toBufferList()
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
