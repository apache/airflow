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
import org.apache.airflow.sdk.execution.api.model.ConnectionResponse
import org.apache.airflow.sdk.execution.api.model.VariableResponse
import org.apache.airflow.sdk.execution.api.model.XComResponse
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream

typealias Decoder = (Map<*, *>) -> Any

object Frame {
  private val mapper =
    ObjectMapper().apply {
      configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      registerModule(JavaTimeModule())
      registerModule(TimestampToJavaOffsetDateTimeModule())
      setDateFormat(StdDateFormat().withColonInTimeZone(true))
    }

  private val inferredTypes =
    mapOf(
      ConnectionResponse::class to "ConnectionResult",
      ErrorResponse::class to "ErrorResponse",
      StartupDetails::class to "StartupDetails",
      VariableResponse::class to "VariableResult",
      XComResponse::class to "XComResult",
    )

  private val toBundleClientTypes: Map<String, Decoder> =
    mapOf(
      "ConnectionResult" to mapperDecoder(ConnectionResponse::class.java),
      "ErrorResponse" to mapperDecoder(ErrorResponse::class.java),
      "VariableResult" to mapperDecoder(VariableResponse::class.java),
      "XComResult" to mapperDecoder(XComResponse::class.java),
    )

  val toTaskTypes: Map<String, Decoder> =
    toBundleClientTypes +
      mapOf(
        "StartupDetails" to mapperDecoder(StartupDetails::class.java),
      )

  // The Java bundle process can act as either Python's DagProcessor or Task runtime, so
  // its inbound decoder is the union of both message sets.
  val toBundleProcessTypes: Map<String, Decoder> = toTaskTypes

  fun encodeRequest(
    id: Int,
    body: Any,
  ): ByteArray = encodeFrame(id, body)

  fun decode(
    bytes: ByteArray,
    bodyTypes: Map<String, Decoder>,
  ): IncomingFrame {
    val unpacker = MessagePack.newDefaultUnpacker(bytes)
    val headerSize = unpacker.unpackArrayHeader()
    check(headerSize >= 2) { "Unexpected Task SDK frame arity $headerSize" }

    val id = unpacker.unpackInt()
    val rawBody = unpacker.unpackAny()
    val rawError = if (headerSize >= 3) unpacker.unpackAny() else null
    unpacker.close()

    val body =
      decodeMessage(rawError, bodyTypes = mapOf("ErrorResponse" to mapperDecoder(ErrorResponse::class.java)))
        ?: decodeMessage(rawBody, bodyTypes)

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

  private fun decodeMessage(
    raw: Any?,
    bodyTypes: Map<String, Decoder>,
  ): Any? {
    val body = raw as? Map<*, *> ?: return raw
    val typeName = body["type"] as? String ?: return body
    val decoder = bodyTypes[typeName] ?: error("Unsupported Task SDK message type $typeName")
    return decoder(body)
  }

  private fun mapperDecoder(targetType: Class<*>): Decoder = { body -> mapper.convertValue(body, targetType) }

  @Suppress("UNCHECKED_CAST")
  private fun toBody(value: Any): Map<String, Any?> =
    when (value) {
      is Map<*, *> -> value as Map<String, Any?>
      else ->
        (mapper.convertValue(value, MutableMap::class.java) as MutableMap<String, Any?>).also { body ->
          inferredTypes[value::class]?.let { typeName -> body.putIfAbsent("type", typeName) }
        }
    }
}
