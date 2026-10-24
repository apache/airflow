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

package org.apache.airflow.sdk.internal

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition
import com.fasterxml.jackson.databind.json.JsonMapper
import java.util.concurrent.ConcurrentHashMap

// Other tasks in the Dag run may read keys that the target class does not
// declare, so this mapper must not fail on an unknown key. By default, Jackson
// moves a date-time that carries an offset to UTC. For example,
// 2024-01-31T02:00+08:00 would become 2024-01-30T18:00Z and fall on another
// date. This mapper keeps the offset as written instead.
private val confMapper: ObjectMapper by lazy {
  JsonMapper
    .builder()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
    .build()
    .findAndRegisterModules()
}

/**
 * Converts a Dag run conf into an instance of [type] for
 * [org.apache.airflow.sdk.DagRun.getConf]. That function documents how keys
 * match fields.
 *
 * Jackson matches a key to a field only when the key is exactly the field
 * name. So this function first uses [foldArgName] to find the field that each
 * key matches, then renames the key to the exact name of that field.
 */
internal fun <T : Any> bindConf(
  conf: Map<String, Any?>,
  type: Class<T>,
): T {
  val javaType = confMapper.constructType(type)
  val matched = matchKeys(conf, javaType)
  return try {
    confMapper.convertValue(matched, javaType)
  } catch (e: IllegalArgumentException) {
    throw IllegalArgumentException("Cannot convert the Dag run conf to ${type.name}: ${e.message}", e)
  }
}

/**
 * Renames the keys of every object in [value] that becomes an instance of a
 * class when [value] is converted to [type]. The renaming also reaches objects
 * in lists and arrays, objects that are map values, and the value inside a
 * reference type such as `Optional`. The keys of a map are data rather than
 * field names, so they stay as written.
 */
private fun matchKeys(
  value: Any?,
  type: JavaType,
): Any? =
  when {
    type.isReferenceType -> matchKeys(value, type.contentType)
    value is Map<*, *> && type.isMapLikeType -> value.mapValues { (_, v) -> matchKeys(v, type.contentType) }
    value is List<*> && (type.isCollectionLikeType || type.isArrayType) -> value.map { matchKeys(it, type.contentType) }
    value is Map<*, *> -> matchFields(value, type)
    else -> value
  }

/**
 * Renames each key of [obj] to the name of the [type] field that the key
 * matches. If a field has exactly the key's name, the key matches that field.
 * Otherwise the key matches the field whose name folds to the same token as the
 * key, unless another key has exactly that field's name. A key that matches no
 * field keeps its name, and Jackson ignores that key.
 *
 * @throws IllegalArgumentException if the names of two fields fold to the same
 *    token, if two keys match the same field only after folding, or if a key
 *    holds null for a primitive field.
 */
private fun matchFields(
  obj: Map<*, *>,
  type: JavaType,
): Map<String, Any?> {
  val fields = findFields(type)
  val className = type.rawClass.simpleName
  val keys = obj.keys.mapTo(mutableSetOf()) { it.toString() }
  val keyOfField = mutableMapOf<String, String>()
  val matched = LinkedHashMap<String, Any?>()
  obj.forEach { (rawKey, value) ->
    val key = rawKey.toString()
    val field = fields.byName[key] ?: fields.byFold[foldArgName(key)]?.takeUnless { it.name in keys }
    if (field == null) {
      matched[key] = value
      return@forEach
    }
    val other = keyOfField.put(field.name, key)
    require(other == null) {
      "Dag run conf keys '$other' and '$key' both match field $className.${field.name} when case and " +
        "underscores are ignored, so the SDK cannot tell which one to use. Name the field exactly as the key " +
        "it should take."
    }
    require(value != null || !field.primaryType.isPrimitive) {
      "Dag run conf key '$key' is null, but field $className.${field.name} has a primitive type that cannot " +
        "be null. Declare a boxed type (e.g. Integer instead of int) to receive null."
    }
    matched[field.name] = matchKeys(value, field.primaryType)
  }
  return matched
}

private class ConfFields(
  val byName: Map<String, BeanPropertyDefinition>,
  val byFold: Map<String, BeanPropertyDefinition>,
)

/**
 * Finds the fields of [type] so that each field can be looked up by its exact
 * name or by its folded token from [foldArgName].
 *
 * The result is cached per type. Jackson takes far longer to find the fields of
 * a class than to convert one object of that class. Without the cache,
 * converting a list of many objects would spend most of the time finding the
 * same fields again.
 *
 * @throws IllegalArgumentException if two fields fold to the same token.
 */
private fun findFields(type: JavaType): ConfFields = confFieldsByType.computeIfAbsent(type) { indexFields(it) }

private val confFieldsByType = ConcurrentHashMap<JavaType, ConfFields>()

private fun indexFields(type: JavaType): ConfFields {
  val fields =
    confMapper.deserializationConfig
      .introspect(type)
      .findProperties()
      .filter { it.isWritable() }
  val className = type.rawClass.simpleName
  val byFold = mutableMapOf<String, BeanPropertyDefinition>()
  fields.forEach { field ->
    val previous = byFold.put(foldArgName(field.name), field)
    require(previous == null) {
      "Fields $className.${previous?.name} and $className.${field.name} differ only in case or underscores, " +
        "so the SDK cannot tell which one a Dag run conf key should go to. Rename one of them."
    }
  }
  return ConfFields(fields.associateBy { it.name }, byFold)
}

/**
 * Whether Jackson can set this property from a key. Jackson also lists
 * read-only properties. For example, a `getBaseURL()` getter next to a
 * `baseUrl` field gives a read-only property named `baseURL`. A read-only
 * property must not take a key, and it must not clash with a field, as
 * `baseURL` would clash with `baseUrl`. A collection or a map that has only a
 * getter is not read-only, because Jackson adds the elements to the object that
 * the getter returns.
 */
private fun BeanPropertyDefinition.isWritable(): Boolean =
  hasSetter() || hasField() || hasConstructorParameter() || getter?.rawType?.let(::isCollectionOrMap) == true

private fun isCollectionOrMap(type: Class<*>): Boolean =
  Collection::class.java.isAssignableFrom(type) || Map::class.java.isAssignableFrom(type)
