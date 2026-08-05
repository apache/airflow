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
import org.apache.airflow.sdk.ArgName
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.In
import org.apache.airflow.sdk.LiteralIn
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.TaskArgs
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.TaskRef
import org.apache.airflow.sdk.execution.ArgBinding
import java.lang.reflect.Field

/**
 * Resolves a task's data parameters and decodes their raw wire values into the
 * declared parameter types. Public so that processor-generated task classes
 * can call it; not user-facing API.
 *
 * Runtime arg bindings from the Python Dag file win over the Java-declared
 * wiring: for a stub task the `@task.stub` call site is the graph the
 * scheduler ordered the run by. Flat data parameters resolve the binding at
 * their position; input-bundle fields resolve bindings by name. Only when the
 * supervisor sent no bindings does resolution fall back to the inputs the
 * `@Wiring` method recorded.
 */
object ArgValues {
  private val mapper: ObjectMapper = JsonMapper.builder().build().findAndRegisterModules()

  /**
   * Materializes a task's declared input: a [TaskArgs] view over the bindings,
   * or a [TaskInput] bundle with every field bound by its wire name.
   *
   * The single populator behind both authoring APIs — the annotation processor
   * emits a call to it for a `@Builder.Task` bundle parameter, and
   * [org.apache.airflow.sdk.InputTask] calls it before handing the input to a
   * task written against the interface.
   *
   * @throws IllegalArgumentException if the bundle cannot be populated.
   * @throws MissingXComException if a primitive field's binding resolves to
   *    nothing, or the wired input a bundle decodes from resolves to nothing.
   */
  @JvmStatic
  fun <I : TaskInput> bindInput(
    context: Context,
    client: Client,
    type: Class<I>,
  ): I {
    if (type == TaskArgs::class.java) return type.cast(TaskArgs(context, client))
    // Runtime bindings bind the bundle's fields by wire name. The Java-wired
    // fallback has no names to bind by, so the bundle decodes wholesale from
    // the single input wired to it -- a bundle is a task's only data parameter.
    if (!hasRuntimeBindings(client)) {
      val resolved = resolveAt(context, client, 0)
      return decode(resolved.value, type) ?: throw resolved.missing(type.simpleName)
    }
    val input = newInput(type)
    bindableFields(type).forEach { field ->
      field.isAccessible = true
      field.set(input, resolveField(client, field))
    }
    return input
  }

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
    val resolved = resolveAt(context, client, position)
    return decode(resolved.value, type) ?: throw resolved.missing(paramName)
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
  ): T? = decode(resolveAt(context, client, position).value, type)

  /**
   * Whether the supervisor delivered TaskFlow arg bindings for this run.
   * [bindInput] branches on this: bindings bind a bundle's fields by name, the
   * wiring fallback decodes the bundle wholesale.
   */
  @JvmStatic
  fun hasRuntimeBindings(client: Client): Boolean = client.argBindings.isNotEmpty()

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

  /**
   * Resolves one bundle field, bound by its [ArgName] value or its verbatim
   * field name. A primitive field cannot hold null, so it fails with a clear
   * [MissingXComException] when the binding resolves to nothing; boxed and
   * reference fields receive null instead.
   */
  @Suppress("UNCHECKED_CAST")
  private fun resolveField(
    client: Client,
    field: Field,
  ): Any? {
    val wireName = field.getAnnotation(ArgName::class.java)?.value ?: field.name
    // The msgpack decoder yields boxed values, so a primitive field decodes
    // into its wrapper and unboxes on assignment.
    val type = field.type.kotlin.javaObjectType as Class<Any>
    return if (field.type.isPrimitive) {
      requiredNamed(client, wireName, type, field.name)
    } else {
      optionalNamed(client, wireName, type)
    }
  }

  /** A resolved raw value plus the error to raise when it is null. */
  private class Resolved(
    val value: Any?,
    val missing: (String) -> MissingXComException,
  )

  private fun resolveAt(
    context: Context,
    client: Client,
    position: Int,
  ): Resolved {
    val bindings = client.argBindings
    if (bindings.isNotEmpty()) {
      check(position < bindings.size) {
        "Task '${context.ti.taskId}' declares a data parameter at position $position " +
          "but the stub call bound only ${bindings.size} argument(s)"
      }
      val binding = bindings[position]
      return Resolved(client.resolveBinding(binding)) { missing(binding, it) }
    }
    val input = inputAt(context, position)
    return Resolved(resolveInput(input, client)) { missing(input, it) }
  }

  private fun inputAt(
    context: Context,
    position: Int,
  ): In<*> {
    val inputs =
      checkNotNull(context.taskDef?.inputs) {
        "Task '${context.ti.taskId}' declares data parameters but has no wired inputs; " +
          "register it through a @Wiring method"
      }
    check(position < inputs.size) {
      "Task '${context.ti.taskId}' declares a data parameter at position $position " +
        "but only ${inputs.size} input(s) are wired"
    }
    return inputs[position]
  }

  private fun resolveInput(
    input: In<*>,
    client: Client,
  ): Any? =
    when (input) {
      is TaskRef<*> -> client.getXCom(taskId = input.def.id)
      is LiteralIn<*> -> input.value
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

  private fun missing(
    input: In<*>,
    target: String,
  ): MissingXComException =
    when (input) {
      is TaskRef<*> -> MissingXComException(input.def.id, target)
      is LiteralIn<*> ->
        MissingXComException(
          "'$target' has a primitive type but its wired literal input is null; " +
            "declare a boxed type (e.g. Integer instead of int) to receive null.",
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
