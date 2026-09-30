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

@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package org.apache.airflow.sdk.internal

import java.lang.reflect.GenericArrayType
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.UUID

private typealias Schema = Map<String, Any?>

private val UNCONSTRAINED: Schema = emptyMap()

/**
 * Builds the JSON Schema of the values a parameter or field of [type]
 * accepts, in the vocabulary pydantic emits for a Python annotation, or null
 * when [type] does not say.
 *
 * The schema describes the declared type, not every value the decoder
 * coerces into it: [ArgValues] also turns `"5"` into a `long`, as pydantic's
 * lax mode does for `int`. Only a primitive excludes null, which the decoder
 * does enforce.
 */
internal fun buildValueSchema(type: Type): Schema? {
  val schema = schemaOf(type) ?: return null
  return if (type is Class<*> && type.isPrimitive) schema else nullable(schema)
}

private fun nullable(schema: Schema): Schema = mapOf("anyOf" to listOf(schema, mapOf("type" to "null")))

private fun schemaOf(type: Type): Schema? =
  when (type) {
    is Class<*> -> classSchema(type)
    is ParameterizedType -> parameterizedSchema(type)
    is GenericArrayType -> arraySchema(type.genericComponentType)
    is WildcardType -> type.upperBounds.firstOrNull()?.let(::schemaOf)
    else -> null // A type variable says nothing about its values.
  }

private fun integer(format: String): Schema = mapOf("type" to "integer", "format" to format)

private fun integerRange(
  minimum: Long,
  maximum: Long,
): Schema = mapOf("type" to "integer", "minimum" to minimum, "maximum" to maximum)

private fun number(format: String): Schema = mapOf("type" to "number", "format" to format)

private fun string(format: String): Schema = mapOf("type" to "string", "format" to format)

private val SCALARS: Map<Class<*>, Schema> =
  buildMap {
    fun put(
      primitive: Class<*>?,
      boxed: Class<*>,
      schema: Schema,
    ) {
      primitive?.let { put(it, schema) }
      put(boxed, schema)
    }
    put(java.lang.Boolean.TYPE, java.lang.Boolean::class.java, mapOf("type" to "boolean"))
    put(java.lang.Byte.TYPE, java.lang.Byte::class.java, integerRange(Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong()))
    put(java.lang.Short.TYPE, java.lang.Short::class.java, integerRange(Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong()))
    put(java.lang.Integer.TYPE, java.lang.Integer::class.java, integer("int32"))
    put(java.lang.Long.TYPE, java.lang.Long::class.java, integer("int64"))
    put(java.lang.Float.TYPE, java.lang.Float::class.java, number("float"))
    put(java.lang.Double.TYPE, java.lang.Double::class.java, number("double"))
    put(
      java.lang.Character.TYPE,
      java.lang.Character::class.java,
      mapOf("type" to "string", "minLength" to 1, "maxLength" to 1),
    )
    put(null, java.lang.String::class.java, mapOf("type" to "string"))
    put(null, BigInteger::class.java, mapOf("type" to "integer"))
    put(null, BigDecimal::class.java, mapOf("type" to "number"))
    put(null, java.lang.Number::class.java, mapOf("type" to "number"))
    put(null, UUID::class.java, string("uuid"))
    put(null, OffsetDateTime::class.java, string("date-time"))
    put(null, ZonedDateTime::class.java, string("date-time"))
    put(null, LocalDateTime::class.java, string("date-time"))
    put(null, Instant::class.java, string("date-time"))
    put(null, LocalDate::class.java, string("date"))
    put(null, LocalTime::class.java, string("time"))
    put(null, Duration::class.java, string("duration"))
  }

private fun classSchema(type: Class<*>): Schema? {
  SCALARS[type]?.let { return it }
  return when {
    // A byte array decodes from a base64 string as well as from a list of numbers.
    type == ByteArray::class.java -> null
    type.isArray -> arraySchema(type.componentType)
    type.isEnum -> enumSchema(type)
    Collection::class.java.isAssignableFrom(type) -> mapOf("type" to "array", "items" to UNCONSTRAINED)
    Map::class.java.isAssignableFrom(type) -> mapOf("type" to "object", "additionalProperties" to true)
    // Other platform types (Object, Optional, Date, URI, ...) decode from more
    // than one JSON shape, or from none, so they constrain nothing here.
    isPlatformType(type) -> null
    // A POJO's fields are not described: the schema says only that it takes an object.
    else -> mapOf("type" to "object")
  }
}

private fun isPlatformType(type: Class<*>): Boolean =
  type.name.startsWith("java.") || type.name.startsWith("javax.") || type.name.startsWith("kotlin.")

// Read from the constant fields rather than enumConstants, which would run the
// enum's static initializer while the bundle is only being described. getFields
// has no defined order, so the names are sorted.
private fun enumSchema(type: Class<*>): Schema =
  mapOf(
    "type" to "string",
    "enum" to
      type.fields
        .filter { it.isEnumConstant }
        .map { it.name }
        .sorted(),
  )

private fun arraySchema(component: Type): Schema = mapOf("type" to "array", "items" to (buildValueSchema(component) ?: UNCONSTRAINED))

private fun parameterizedSchema(type: ParameterizedType): Schema? {
  val raw = type.rawType as? Class<*> ?: return null
  val arguments = type.actualTypeArguments
  return when {
    Collection::class.java.isAssignableFrom(raw) && arguments.size == 1 -> arraySchema(arguments[0])
    Map::class.java.isAssignableFrom(raw) && arguments.size == 2 ->
      mapOf("type" to "object", "additionalProperties" to (buildValueSchema(arguments[1]) ?: true))
    else -> classSchema(raw)
  }
}
