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
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream

typealias TaskSdkMessageDecoder = (Map<*, *>) -> Any

object TaskSdkFrames {
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

  private val toBundleClientTypes: Map<String, TaskSdkMessageDecoder> =
    mapOf(
      "ConnectionResult" to mapperDecoder(ConnectionResponse::class.java),
      "ErrorResponse" to mapperDecoder(ErrorResponse::class.java),
      "VariableResult" to mapperDecoder(VariableResponse::class.java),
      "XComResult" to mapperDecoder(XComResponse::class.java),
    )

  val toDagProcessorTypes: Map<String, TaskSdkMessageDecoder> =
    toBundleClientTypes +
      mapOf(
        "DagFileParseRequest" to mapperDecoder(DagFileParseRequest::class.java),
      )

  val toTaskTypes: Map<String, TaskSdkMessageDecoder> =
    toBundleClientTypes +
      mapOf(
        "StartupDetails" to mapperDecoder(StartupDetails::class.java),
      )

  // The Java bundle process can act as either Python's DagProcessor or Task runtime, so
  // its inbound decoder is the union of both message sets.
  val toBundleProcessTypes: Map<String, TaskSdkMessageDecoder> = toDagProcessorTypes + toTaskTypes

  val toSupervisorTypes: Map<String, TaskSdkMessageDecoder> =
    mapOf(
      "ErrorResponse" to mapperDecoder(ErrorResponse::class.java),
      "GetConnection" to { body -> GetConnection(id = body.string("conn_id")) },
      "GetVariable" to { body -> GetVariable(key = body.string("key")) },
      "GetXCom" to {
        GetXCom(
          key = it.string("key"),
          dagId = it.string("dag_id"),
          taskId = it.string("task_id"),
          runId = it.string("run_id"),
          mapIndex = it.intOrNull("map_index"),
          includePriorDates = it.boolean("include_prior_dates", default = false),
        )
      },
      "SetXCom" to {
        SetXCom(
          key = it.string("key"),
          value = it["value"] ?: error("Missing 'value'"),
          dagId = it.string("dag_id"),
          taskId = it.string("task_id"),
          runId = it.string("run_id"),
          mapIndex = it.int("map_index"),
        )
      },
      "SucceedTask" to { SucceedTask() },
      "TaskState" to { body -> TaskState(state = body.string("state")) },
    )

  fun encodeRequest(
    id: Int,
    body: Any,
  ): ByteArray = encodeFrame(id, body, error = null, isResponse = false)

  fun encodeResponse(
    id: Int,
    body: Any? = null,
    error: ErrorResponse? = null,
  ): ByteArray = encodeFrame(id, body, error = error, isResponse = true)

  fun writeRequest(
    output: OutputStream,
    id: Int,
    body: Any,
  ) = writeFrame(output, encodeRequest(id, body))

  fun writeResponse(
    output: OutputStream,
    id: Int,
    body: Any? = null,
    error: ErrorResponse? = null,
  ) = writeFrame(output, encodeResponse(id, body, error))

  fun decode(
    bytes: ByteArray,
    bodyTypes: Map<String, TaskSdkMessageDecoder>,
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

  fun readFrame(
    input: InputStream,
    bodyTypes: Map<String, TaskSdkMessageDecoder>,
  ): IncomingFrame = decode(readBytes(input, readLengthPrefix(input)), bodyTypes)

  fun lengthPrefix(length: Int) =
    byteArrayOf(
      (length shr 24).toByte(),
      (length shr 16).toByte(),
      (length shr 8).toByte(),
      length.toByte(),
    )

  fun readLengthPrefix(input: InputStream): Int = parseLengthPrefix(readBytes(input, 4))

  fun parseLengthPrefix(prefix: ByteArray): Int {
    check(prefix.size == 4) { "Need 4 prefix bytes" }
    return prefix.fold(0) { acc, byte -> (acc shl 8) or (byte.toInt() and 0xff) }
  }

  fun readBytes(
    input: InputStream,
    length: Int,
  ): ByteArray {
    val bytes = input.readNBytes(length)
    if (bytes.size != length) {
      throw EOFException("Expected $length bytes but only received ${bytes.size}")
    }
    return bytes
  }

  private fun writeFrame(
    output: OutputStream,
    payload: ByteArray,
  ) {
    output.write(lengthPrefix(payload.size))
    output.write(payload)
    output.flush()
  }

  private fun encodeFrame(
    id: Int,
    body: Any?,
    error: ErrorResponse?,
    isResponse: Boolean,
  ): ByteArray {
    val payload = ByteArrayOutputStream()
    val packer = MessagePack.newDefaultPacker(payload)
    packer.packArrayHeader(if (isResponse) 3 else 2)
    packer.packInt(id)
    packer.packAny(body?.let(::toBody))
    if (isResponse) {
      packer.packAny(error?.let(::toBody))
    }
    packer.close()
    return payload.toByteArray()
  }

  private fun decodeMessage(
    raw: Any?,
    bodyTypes: Map<String, TaskSdkMessageDecoder>,
  ): Any? {
    val body = raw as? Map<*, *> ?: return raw
    val typeName = body["type"] as? String ?: return body
    val decoder = bodyTypes[typeName] ?: error("Unsupported Task SDK message type $typeName")
    return decoder(body)
  }

  private fun mapperDecoder(targetType: Class<*>): TaskSdkMessageDecoder = { body -> mapper.convertValue(body, targetType) }

  @Suppress("UNCHECKED_CAST")
  private fun toBody(value: Any): Map<String, Any?> =
    when (value) {
      is Map<*, *> -> value as Map<String, Any?>
      else ->
        (mapper.convertValue(value, MutableMap::class.java) as MutableMap<String, Any?>).also { body ->
          inferredTypes[value::class]?.let { typeName -> body.putIfAbsent("type", typeName) }
        }
    }

  private fun Map<*, *>.string(key: String): String = this[key] as? String ?: error("Missing '$key'")

  private fun Map<*, *>.int(key: String): Int = intOrNull(key) ?: error("Missing integer '$key'")

  private fun Map<*, *>.intOrNull(key: String): Int? =
    when (val value = this[key]) {
      null -> null
      is Number -> value.toInt()
      else -> error("Expected integer '$key', got ${value::class.java}")
    }

  private fun Map<*, *>.boolean(
    key: String,
    default: Boolean,
  ): Boolean =
    when (val value = this[key]) {
      null -> default
      is Boolean -> value
      else -> error("Expected boolean '$key', got ${value::class.java}")
    }
}
