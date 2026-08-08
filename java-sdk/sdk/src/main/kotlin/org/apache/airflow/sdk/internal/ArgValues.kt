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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.execution.ArgBinding

/**
 * Resolves a task's data parameters from the arg bindings the supervisor
 * delivered, and decodes their raw wire values into the declared parameter
 * types. Public so that processor-generated task classes can call it; not
 * user-facing API.
 *
 * The bindings come from the Python `@task.stub` call site, which is also the
 * graph the scheduler ordered the run by. Flat data parameters resolve the
 * binding at their position; input-bundle fields resolve bindings by name.
 */
object ArgValues {
  private val mapper: ObjectMapper = JsonMapper.builder().build().findAndRegisterModules()

  /**
   * Resolves the data parameter at [position] into [type] for a parameter that
   * cannot be null.
   *
   * @param position Zero-based index among the task's data parameters, in
   *    declaration order.
   * @throws MissingXComException if the resolved value is null.
   */
  @JvmStatic
  fun <T : Any> requiredInput(
    context: Context,
    client: Client,
    position: Int,
    type: Class<T>,
    paramName: String,
  ): T {
    val binding = bindingAt(context, client, position)
    return decode(client.resolveBinding(binding), type) ?: throw missing(binding, paramName)
  }

  /**
   * Resolves the data parameter at [position] into [type], passing null
   * through.
   *
   * @param position Zero-based index among the task's data parameters, in
   *    declaration order.
   */
  @JvmStatic
  fun <T : Any> optionalInput(
    context: Context,
    client: Client,
    position: Int,
    type: Class<T>,
  ): T? = decode(client.resolveBinding(bindingAt(context, client, position)), type)

  /**
   * Resolves the runtime binding named [name] into [type] for an input-bundle
   * field that cannot be null.
   *
   * @param name Wire name of the argument (`@ArgName` value or the verbatim
   *    field name).
   * @throws IllegalStateException if the stub call bound no argument named
   *    [name].
   * @throws MissingXComException if the resolved value is null.
   */
  @JvmStatic
  fun <T : Any> requiredNamed(
    client: Client,
    name: String,
    type: Class<T>,
    fieldName: String,
  ): T {
    val binding =
      checkNotNull(client.argBindings.firstOrNull { it.name == name }) {
        "The stub call bound no argument named '$name', required by input field '$fieldName'"
      }
    return decode(client.resolveBinding(binding), type) ?: throw missing(binding, fieldName, name)
  }

  /**
   * Resolves the runtime binding named [name] into [type], passing null
   * through. An absent binding resolves to null.
   *
   * @param name Wire name of the argument (`@ArgName` value or the verbatim
   *    field name).
   */
  @JvmStatic
  fun <T : Any> optionalNamed(
    client: Client,
    name: String,
    type: Class<T>,
  ): T? {
    val binding = client.argBindings.firstOrNull { it.name == name } ?: return null
    return decode(client.resolveBinding(binding), type)
  }

  private fun bindingAt(
    context: Context,
    client: Client,
    position: Int,
  ): ArgBinding {
    val bindings = client.argBindings
    check(position < bindings.size) {
      "Task '${context.ti.taskId}' declares a data parameter at position $position " +
        "but the stub call bound only ${bindings.size} argument(s)"
    }
    return bindings[position]
  }

  private fun missing(
    binding: ArgBinding,
    target: String,
    argName: String? = null,
  ): MissingXComException =
    when (binding) {
      is ArgBinding.XCom -> MissingXComException(binding.taskId, target)
      is ArgBinding.Literal ->
        MissingXComException(
          "'$target' has a primitive type but the stub call bound a null literal" +
            (argName?.let { " for argument '$it'" } ?: "") +
            "; declare a boxed type (e.g. Integer instead of int) to receive null.",
        )
    }

  internal fun <T : Any> decode(
    value: Any?,
    type: Class<T>,
  ): T? {
    if (value == null) return null
    if (type.isInstance(value)) return type.cast(value)
    // The msgpack decoder yields Long for wire integers and Double for wire
    // floats, so widen numerics via Number instead of casting.
    if (value is Number) {
      numberConverter(type)?.let { return type.cast(it(value)) }
    }
    // Structured wire values (maps, lists) convert into the declared POJO or
    // collection type; unknown fields fail the task, mirroring the Go SDK's
    // strict decode of task inputs.
    return mapper.convertValue(value, type)
  }

  private fun numberConverter(type: Class<*>): ((Number) -> Any)? =
    when (type) {
      java.lang.Byte::class.java -> { n -> n.toByte() }
      java.lang.Short::class.java -> { n -> n.toShort() }
      java.lang.Integer::class.java -> { n -> n.toInt() }
      java.lang.Long::class.java -> { n -> n.toLong() }
      java.lang.Float::class.java -> { n -> n.toFloat() }
      java.lang.Double::class.java -> { n -> n.toDouble() }
      else -> null
    }
}
