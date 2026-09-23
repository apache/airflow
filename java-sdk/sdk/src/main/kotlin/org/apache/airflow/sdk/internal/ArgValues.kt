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
import org.apache.airflow.sdk.Arg
import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.LiteralArg
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.TaskRef
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
 *
 * A natively authored Dag has no stub call site, so the supervisor sends no
 * bindings for it and the inputs the Dag itself wired stand in. When the
 * supervisor sends bindings they are used for every parameter; the Dag's own
 * inputs are read only when it sends none.
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
    // Runtime bindings carry argument names to match fields against. A wired
    // input carries none, so it decodes into the whole input at once -- which
    // is well defined because a TaskInput is a task's only data parameter.
    wiredInputs(context, client)?.let { wired ->
      return decode(resolveWired(wired[0], client), type) as I?
        ?: throw missingInput(wired[0], type.simpleName)
    }
    val input = newInput(type)
    val arguments = ArgIndex(client.argBindings)
    bindableFields(type).forEach { field -> field.set(input, resolveField(client, arguments, field)) }
    return input
  }

  /**
   * Resolves the data parameter at [position] into [type], passing null
   * through. Backs [TaskArgs]; a parameter that cannot be null goes through
   * [TaskArgs.require], which turns null into [missing]. [TaskArgs.of] has
   * already matched the declared parameters against the bindings, so a
   * position always names one.
   *
   * @param position Zero-based index among the task's data parameters, in
   *    declaration order.
   */
  internal fun valueAt(
    context: Context,
    client: Client,
    position: Int,
    type: Type,
  ): Any? {
    val wired = wiredInputs(context, client)
    return if (wired != null) {
      decode(resolveWired(wired[position], client), type)
    } else {
      decode(client.resolveBinding(client.argBindings[position]), type)
    }
  }

  /**
   * The inputs the Dag wired for this task, or null when the run's arguments
   * come from the stub call site. A task with no wired inputs reads the
   * bindings, so a stub call that bound nothing keeps its own diagnostics.
   */
  internal fun wiredInputs(
    context: Context,
    client: Client,
  ): List<Arg<*>>? =
    if (client.argBindings.isEmpty()) context.taskDef?.inputs?.takeIf { it.isNotEmpty() } else null

  /** How many arguments this run supplies, from whichever source supplies them. */
  internal fun suppliedCount(
    context: Context,
    client: Client,
  ): Int = wiredInputs(context, client)?.size ?: client.argBindings.size

  /**
   * The failure for a wired argument that resolved to nothing where a value is
   * required, naming [target] — the position of the parameter it feeds.
   */
  internal fun missingWired(
    input: Arg<*>,
    target: String,
  ): MissingXComException =
    when (input) {
      is TaskRef<*> -> MissingXComException(input.def.id, target)
      else ->
        MissingXComException(
          "Task parameter '$target' is wired to a null literal, but has a primitive type that cannot " +
            "be null; declare a boxed type (e.g. Integer instead of int) to receive null.",
        )
    }

  /** The failure for a wired input that resolved to nothing for a [TaskInput]. */
  private fun missingInput(
    input: Arg<*>,
    target: String,
  ): MissingXComException =
    when (input) {
      is TaskRef<*> ->
        MissingXComException(
          "Input '$target' requires an XCom from task '${input.def.id}', but none was pushed.",
        )
      else -> MissingXComException("Input '$target' is wired to a null literal, so there is nothing to bind.")
    }

  private fun resolveWired(
    input: Arg<*>,
    client: Client,
  ): Any? =
    when (input) {
      is TaskRef<*> -> client.getXCom(taskId = input.def.id)
      is LiteralArg<*> -> input.value
      else -> null
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
