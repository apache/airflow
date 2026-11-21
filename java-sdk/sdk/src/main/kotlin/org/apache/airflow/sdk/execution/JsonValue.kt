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

/**
 * Checks that [value] can be stored as an XCom or task-state value, and returns
 * it unchanged so later encoding matches the previous Jackson behaviour.
 *
 * [Frame] encodes a request by converting the message object to a map with
 * Jackson. A Scala collection, `Option` or case class (none of which are
 * `java.util.Map` / `java.util.Collection`) was stored as that object's bean
 * properties, or as `{}` when it had none, and the task succeeded. This
 * inspects the original value first, and rejects those Scala types and any
 * other object Jackson would encode as `{}`. Java types that already encoded
 * usefully (POJOs and Kotlin data classes with properties, records, enums,
 * `java.time`, `Date`, `Optional`, and so on) still pass through.
 *
 * Nested values in a `Map`, `Collection` or array are checked the same way.
 */
internal fun jsonValue(value: Any?): Any? {
  rejectUnsupportedXComValue(value)
  return value
}

private fun rejectUnsupportedXComValue(value: Any?) {
  when (value) {
    null, is String, is Boolean, is Number -> return
    is ByteArray ->
      throw IllegalArgumentException(
        "Airflow does not accept byte arrays because they are not JSON values. " +
          "Encode the bytes as a string first, for example with java.util.Base64.",
      )
    is Map<*, *> -> value.values.forEach(::rejectUnsupportedXComValue)
    is Collection<*> -> value.forEach(::rejectUnsupportedXComValue)
    is Array<*> -> value.forEach(::rejectUnsupportedXComValue)
    is IntArray, is LongArray, is ShortArray, is DoubleArray, is FloatArray, is BooleanArray, is CharArray ->
      return
    else -> {
      if (isScalaType(value.javaClass)) {
        throw IllegalArgumentException(unsupportedScalaXComMessage(value))
      }
      if (Frame.encodesAsEmptyObject(value)) {
        throw IllegalArgumentException(emptyObjectXComMessage(value))
      }
    }
  }
}

/**
 * True when [type] is a Scala type, or implements or extends one, detected by
 * a `scala.` name prefix so the SDK does not depend on the Scala library.
 * That covers `scala.collection.*`, `scala.Option` / `Some` / `None`,
 * `scala.Product` (case classes) and `scala.Tuple*`.
 */
internal fun isScalaType(type: Class<*>): Boolean {
  val seen = HashSet<Class<*>>()
  val queue = ArrayDeque<Class<*>>()
  queue.add(type)
  while (queue.isNotEmpty()) {
    val current = queue.removeFirst()
    if (!seen.add(current)) continue
    if (current.name.startsWith("scala.")) return true
    current.superclass?.let { queue.add(it) }
    queue.addAll(current.interfaces)
  }
  return false
}

private fun unsupportedScalaXComMessage(value: Any): String =
  "Unsupported XCom value type: ${value.javaClass.name}. " +
    "Scala collections, Option and case classes cannot be stored as XComs directly. " +
    "Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), " +
    "and convert a case class or Option to a java.util.Map or list."

private fun emptyObjectXComMessage(value: Any): String =
  "Unsupported XCom value type: ${value.javaClass.name}. " +
    "This value has no serializable properties and would be stored as {}. " +
    "Pass a java.util.Map, list, or JSON scalar. " +
    "Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), " +
    "and convert a case class to a java.util.Map or list."
