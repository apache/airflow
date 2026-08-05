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
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.execution.ArgBinding
import java.lang.reflect.Field
import java.lang.reflect.Type

/**
 * @suppress
 *
 * Resolves a task's data parameters from the arg bindings the supervisor
 * delivered, and decodes their raw wire values into the declared types. Public
 * so that processor-generated task classes can call it; not user-facing API.
 *
 * The bindings come from the Python `@task.stub` call site, which is also the
 * graph the scheduler ordered the run by. Flat data parameters resolve the
 * binding at their position (through [TaskArgs]); [TaskInput] fields resolve
 * bindings by name.
 */
object ArgValues {
  private val mapper: ObjectMapper = JsonMapper.builder().build().findAndRegisterModules()

  /**
   * Materializes a [TaskInput] with every field bound by the argument name it
   * claims.
   *
   * The single populator behind both authoring APIs — the annotation processor
   * emits a call to it for a `@Builder.Task` [TaskInput] parameter, and
   * [org.apache.airflow.sdk.InputTask] calls it before handing the input to a
   * task written against the interface.
   *
   * @throws IllegalArgumentException if the input cannot be populated.
   * @throws MissingXComException if a primitive field's binding resolves to
   *    nothing.
   */
  @JvmStatic
  fun <I : TaskInput> bindInput(
    context: Context,
    client: Client,
    type: Class<I>,
  ): I {
    val input = newInput(type)
    val arguments = ArgIndex(client.argBindings)
    bindableFields(type).forEach { field ->
      field.isAccessible = true
      field.set(input, resolveField(client, arguments, field))
    }
    return input
  }

  /**
   * Resolves the data parameter at [position] into [type], passing null
   * through. Backs [TaskArgs]; a parameter that cannot be null goes through
   * [TaskArgs.require], which turns null into [missing].
   *
   * @param position Zero-based index among the task's data parameters, in
   *    declaration order.
   * @throws IllegalStateException if the call site bound no argument there.
   */
  internal fun valueAt(
    context: Context,
    client: Client,
    position: Int,
    type: Type,
  ): Any? {
    val bindings = client.argBindings
    check(position < bindings.size) {
      "Task '${context.ti.taskId}' declares a data parameter at position $position " +
        "but the stub call bound only ${bindings.size} argument(s)"
    }
    return decode(client.resolveBinding(bindings[position]), type)
  }

  /**
   * Builds the failure for a binding that resolved to nothing where a value is
   * required, naming [target] — the stub argument, or the [TaskInput] field
   * that claimed it.
   */
  internal fun missing(
    binding: ArgBinding,
    taskId: String,
    target: String = binding.name,
  ): MissingXComException =
    when (binding) {
      is ArgBinding.XCom -> MissingXComException(binding.taskId, target)
      is ArgBinding.Literal ->
        MissingXComException(
          "Task parameter '$target' of task '$taskId' is bound to a null literal, but has a primitive " +
            "type that cannot be null; declare a boxed type (e.g. Integer instead of int) to receive null.",
        )
    }

  /**
   * Resolves one [TaskInput] field from the argument it claims. A primitive
   * field cannot hold null, so it fails with a clear [MissingXComException]
   * when the binding resolves to nothing; boxed and reference fields receive
   * null instead.
   */
  private fun resolveField(
    client: Client,
    arguments: ArgIndex,
    field: Field,
  ): Any? {
    val argName = argNameOf(field)
    val binding = arguments.find(argName, pinned = isPinned(field))
    if (!field.type.isPrimitive) return binding?.let { decode(client.resolveBinding(it), field.genericType) }

    checkNotNull(binding) {
      "The stub call bound no argument named '$argName', required by input field '${field.name}'"
    }
    // The msgpack decoder yields boxed values, so a primitive field decodes
    // into its wrapper and unboxes on assignment.
    return decode(client.resolveBinding(binding), field.type.kotlin.javaObjectType)
      ?: throw missing(binding, client.details.ti.taskId, field.name)
  }

  /**
   * Decodes a raw wire value into [type], which carries the full generic type
   * where the declared type has one, so an element type survives the decode.
   */
  internal fun decode(
    value: Any?,
    type: Type,
  ): Any? {
    if (value == null) return null
    if (type is Class<*>) {
      if (type.isInstance(value)) return value
      // The msgpack decoder yields Long for wire integers and Double for wire
      // floats, so widen numerics via Number instead of casting.
      if (value is Number) numberConverter(type)?.let { return it(value) }
    }
    // Structured wire values (maps, lists) convert into the declared POJO or
    // collection type; unknown fields fail the task, mirroring the Go SDK's
    // strict decode of task inputs.
    return mapper.convertValue(value, mapper.constructType(type))
  }

  /**
   * The run's bindings, addressable by the exact argument name and by the
   * cross-language fold. A fold two arguments share is dropped rather than
   * guessed at: either could be the one meant, and handing a field the wrong
   * value is worse than not binding it.
   */
  private class ArgIndex(
    bindings: List<ArgBinding>,
  ) {
    private val byName = bindings.associateBy { it.name }
    private val byFold = mutableMapOf<String, ArgBinding>()
    private val sharedFolds = mutableSetOf<String>()

    init {
      bindings.forEach { binding ->
        val fold = foldArgName(binding.name)
        if (byFold.put(fold, binding) != null) sharedFolds += fold
      }
    }

    /**
     * Finds the argument a field claims. An `@ArgName`-pinned name is taken
     * literally, which is what makes the annotation an escape hatch for a
     * Python name no Java identifier folds to.
     */
    fun find(
      name: String,
      pinned: Boolean,
    ): ArgBinding? {
      byName[name]?.let { return it }
      if (pinned) return null
      val fold = foldArgName(name)
      return if (fold in sharedFolds) null else byFold[fold]
    }
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
