package org.apache.airflow.sdk.execution

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.util.JacksonFeatureSet
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeFeature
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer
import org.msgpack.core.ExtensionTypeHeader
import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import org.msgpack.jackson.dataformat.MessagePackExtensionType
import org.msgpack.value.ArrayValue
import org.msgpack.value.MapValue
import org.msgpack.value.Value
import org.msgpack.value.ValueType
import java.math.BigInteger
import java.time.OffsetDateTime
import java.time.ZoneOffset

private fun MessagePacker.packByteArray(data: ByteArray) {
  packBinaryHeader(data.size)
  data.forEach { packByte(it) }
}

private fun MessagePacker.packMap(data: Map<*, *>) {
  packMapHeader(data.size)
  data.forEach { (k, v) ->
    check(k is String)
    packString(k)
    packAny(v)
  }
}

private fun MessagePacker.packCollection(data: Collection<*>) {
  packArrayHeader(data.size)
  data.forEach { packAny(it) }
}

fun MessagePacker.packAny(data: Any?) {
  when (data) {
    null -> packNil()
    is Boolean -> packBoolean(data)
    is Byte -> packByte(data)
    is Short -> packShort(data)
    is Int -> packInt(data)
    is Long -> packLong(data)
    is BigInteger -> packBigInteger(data)
    is Float -> packFloat(data)
    is Double -> packDouble(data)
    is ByteArray -> packByteArray(data)
    is String -> packString(data)
    is Map<*, *> -> packMap(data)
    is Collection<*> -> packCollection(data)
    else -> throw IllegalArgumentException("Unsupported data type: $data")
  }
}

private fun ArrayValue.decodeArray(): List<*> =
  mutableListOf<Any?>().also {
    iterator().forEach { v -> it.add(v.decode()) }
  }

private fun MapValue.decodeMap(): Map<*, *> =
  mutableMapOf<String, Any?>().also {
    entrySet().forEach { (k, v) -> it[k.asStringValue().asString()] = v.decode() }
  }

private fun Value.decode(): Any? =
  when (valueType) {
    ValueType.NIL -> null
    ValueType.BOOLEAN -> asBooleanValue().boolean
    ValueType.INTEGER ->
      with(asIntegerValue()) {
        if (isInLongRange) asLong() else asBigInteger()
      }
    ValueType.FLOAT -> asFloatValue().toDouble()
    ValueType.STRING -> asStringValue().asString()
    ValueType.BINARY -> asBinaryValue().asByteArray()
    ValueType.ARRAY -> asArrayValue().decodeArray()
    ValueType.MAP -> asMapValue().decodeMap()
    else -> throw IllegalArgumentException("Unsupported data type: $this")
  }

fun MessageUnpacker.unpackAny(): Any? = unpackValue().decode()

class TimestampToJavaOffsetDateTimeModule : SimpleModule() {
  companion object {
    const val EXT_TYPE: Byte = -1
  }

  class OffsetDateTimeDeserializer : StdDeserializer<OffsetDateTime>(OffsetDateTime::class.java) {
    val instantDeserializer =
      InstantDeserializer.OFFSET_DATE_TIME.withFeatures(
        JacksonFeatureSet.fromDefaults(JavaTimeFeature.entries.toTypedArray()),
      )

    override fun deserialize(
      p: JsonParser,
      ctxt: DeserializationContext,
    ): OffsetDateTime {
      if (p.currentToken == JsonToken.VALUE_EMBEDDED_OBJECT) {
        deserializeMsgPackTimestamp(p)?.let { return it }
      }
      return instantDeserializer.deserialize(p, ctxt)
    }

    private fun deserializeMsgPackTimestamp(p: JsonParser): OffsetDateTime? {
      val ext = p.readValueAs(MessagePackExtensionType::class.java)
      if (ext.type != EXT_TYPE) {
        return null
      }
      val unpacker = MessagePack.newDefaultUnpacker(ext.data)
      val instant = unpacker.unpackTimestamp(ExtensionTypeHeader(EXT_TYPE, ext.data.size))
      return instant.atOffset(ZoneOffset.UTC)
    }
  }

  init {
    addDeserializer(OffsetDateTime::class.java, OffsetDateTimeDeserializer())
  }
}
