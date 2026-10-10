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

import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

/**
 * Value shape of one Dag serialization schema key. Public so that the
 * annotation processor can lower `@Builder.Dag` / `@Builder.Task` attributes;
 * not user-facing API.
 */
enum class FieldType {
  STRING,
  INTEGER,
  NUMBER,
  BOOLEAN,
  STRING_ARRAY,
  TIMEDELTA,
  DATETIME,

  /** A list of Dag run states, as `allowed_states` takes. */
  DAG_RUN_STATES,

  /** A map with string keys, as `conf` takes. */
  JSON_OBJECT,
}

/** A Dag run state, as a `DAG_RUN_STATES` field names one. */
private val DAG_RUN_STATE_NAMES = listOf("queued", "running", "success", "failed")

/**
 * One configuration key from the Dag serialization schema. Public so that the
 * annotation processor can lower `@Builder.Dag` / `@Builder.Task` attributes;
 * not user-facing API.
 *
 * @property key Schema property name, e.g. `retry_delay`.
 * @property attribute Annotation attribute name, e.g. `retryDelay`.
 * @property type Accepted value shape.
 * @property defaultJson Schema default as raw JSON, or `null` when the schema
 *    declares no default.
 */
class Field(
  val key: String,
  val attribute: String,
  val type: FieldType,
  val defaultJson: String?,
)

/**
 * Validates one `config(key, value)` call against a schema field table and
 * returns the value to store.
 *
 * @throws IllegalArgumentException if the key is not a configurable schema
 *    key or the value does not match the key's type.
 */
internal fun checkConfigValue(
  scope: String,
  table: Map<String, Field>,
  key: String,
  value: Any?,
): Any {
  val field =
    requireNotNull(table[key]) {
      "Unknown $scope config key: '$key'"
    }
  requireNotNull(value) {
    "Value for $scope config key '$key' must not be null"
  }

  fun mismatch(expected: String): Nothing =
    throw IllegalArgumentException(
      "Value for $scope config key '$key' must be $expected, got: ${value.javaClass.name}",
    )
  return when (field.type) {
    FieldType.STRING -> value as? String ?: mismatch("a String")
    FieldType.BOOLEAN -> value as? Boolean ?: mismatch("a Boolean")
    FieldType.NUMBER -> value as? Number ?: mismatch("a Number")
    FieldType.INTEGER ->
      when (value) {
        is Byte, is Short, is Int, is Long -> value
        else -> mismatch("an integral Number")
      }
    FieldType.TIMEDELTA -> value as? Duration ?: mismatch("a java.time.Duration")
    FieldType.DATETIME ->
      when (value) {
        is OffsetDateTime -> value
        is Instant -> value
        else -> mismatch("a java.time.OffsetDateTime or java.time.Instant")
      }
    FieldType.STRING_ARRAY ->
      when {
        value is Iterable<*> && value.all { it is String } -> value.map { it as String }
        value is Array<*> && value.all { it is String } -> value.map { it as String }
        else -> mismatch("an Iterable of String")
      }
    FieldType.DAG_RUN_STATES -> {
      val states =
        when (value) {
          is Iterable<*> -> value.toList()
          is Array<*> -> value.toList()
          else -> mismatch("an Iterable of Dag run states")
        }
      states.map { state ->
        require(state is String && state in DAG_RUN_STATE_NAMES) {
          "Value for $scope config key '$key' holds $state, which is not a Dag run state; use one " +
            "of ${DAG_RUN_STATE_NAMES.joinToString()}"
        }
        state as String
      }
    }
    FieldType.JSON_OBJECT -> {
      val map = value as? Map<*, *> ?: mismatch("a Map")
      map.entries.associate { (mapKey, entry) ->
        require(mapKey is String) {
          "Value for $scope config key '$key' must have String keys, got: ${mapKey?.javaClass?.name}"
        }
        mapKey to entry
      }
    }
  }
}
