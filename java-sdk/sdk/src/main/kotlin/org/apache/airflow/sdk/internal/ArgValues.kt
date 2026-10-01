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
import org.apache.airflow.sdk.MissingXComException
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.execution.ArgBinding
import org.apache.airflow.sdk.execution.Logger
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
  private val logger = Logger(ArgValues::class)

  /**
   * Materializes a [TaskInput] with every field bound by the argument name it
   * claims.
   *
   * The single populator behind both authoring APIs — the annotation processor
   * emits a call to it for a `@Builder.Task` [TaskInput] parameter, and
   * [org.apache.airflow.sdk.InputTask] calls it before handing the input to a
   * task written against the interface.
   *
   * Every field has to find an argument: a field nothing binds means the input
   * and the stub signature disagree. Neither direction of that disagreement
   * fails the task, because a field binds by name: a field nothing supplies
   * keeps its Java default, and an argument no field claims changes nothing
   * the task reads. Both are logged so the mismatch stays visible.
   *
   * @throws IllegalArgumentException if the input cannot be populated.
   * @throws MissingXComException if a field's argument resolves to nothing and
   *    the field is primitive.
   */
  @JvmStatic
  fun <I : TaskInput> bindInput(
    client: Client,
    type: Class<I>,
  ): I {
    val input = newInput(type)
    val arguments = ArgIndex(client.argBindings)
    val unfilled = mutableListOf<String>()
    bindableFields(type).forEach { field ->
      val argName = argNameOf(field)
      val pinned = isPinned(field)
      val binding = arguments.claim(argName, pinned)
      if (binding == null) {
        unfilled += unfilledField(arguments, field, argName, pinned)
      } else {
        field.set(input, resolveClaimed(client, binding, field))
      }
    }
    warnUnfilled(client, type, unfilled, arguments)
    warnUnclaimed(client, type, arguments)
    return input
  }

  /**
   * Reports the fields the call site supplied nothing for. Each keeps its Java
   * default, so the task runs on a value nobody passed.
   */
  private fun warnUnfilled(
    client: Client,
    type: Class<*>,
    unfilled: List<String>,
    arguments: ArgIndex,
  ) {
    if (unfilled.isEmpty()) return
    logger.warning(
      "Task handler declares argument(s) the Dag's call did not pass",
      mapOf(
        "task_id" to client.details.ti.taskId,
        "input" to type.simpleName,
        "declared_not_passed" to unfilled,
        "passed" to arguments.passed(),
      ),
    )
  }

  /**
   * Reports the arguments the call site passed that no field took. A captured
   * default is not one of them: the Dag author did not write it, so a field
   * has no reason to exist for it.
   */
  private fun warnUnclaimed(
    client: Client,
    type: Class<*>,
    arguments: ArgIndex,
  ) {
    val unclaimed = arguments.unclaimed()
    if (unclaimed.isEmpty()) return
    logger.warning(
      "Dag's call passed argument(s) the task handler does not declare",
      mapOf(
        "task_id" to client.details.ti.taskId,
        "input" to type.simpleName,
        "passed_not_declared" to unclaimed,
        "declared" to bindableFields(type).map(::argNameOf),
      ),
    )
  }

  /**
   * Resolves one data parameter into [type], passing null through. Backs
   * [TaskArgs]; a parameter that cannot be null goes through
   * [TaskArgs.require], which turns null into [missing].
   *
   * @param binding The argument [TaskArgs] holds for that position.
   */
  internal fun valueAt(
    client: Client,
    binding: ArgBinding,
    type: Type,
  ): Any? = decode(client.resolveBinding(binding), type)

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
   * Resolves one [TaskInput] field from the argument it claimed.
   *
   * An argument that resolves to nothing is a value, not a mistake: a boxed or
   * reference field takes null, and a primitive field cannot, so it fails with
   * a clear [MissingXComException].
   */
  private fun resolveClaimed(
    client: Client,
    binding: ArgBinding,
    field: Field,
  ): Any? {
    if (!field.type.isPrimitive) return decode(client.resolveBinding(binding), field.genericType)
    // The msgpack decoder yields boxed values, so a primitive field decodes
    // into its wrapper and unboxes on assignment.
    return decode(client.resolveBinding(binding), field.type.kotlin.javaObjectType)
      ?: throw missing(binding, client.details.ti.taskId, field.name)
  }

  /**
   * Names a field the call site supplied nothing for, saying which of the two
   * reasons it was: no argument of that name, or two that the fold cannot tell
   * apart, where `@ArgName` is the way to say which one is meant.
   */
  private fun unfilledField(
    arguments: ArgIndex,
    field: Field,
    argName: String,
    pinned: Boolean,
  ): String =
    if (arguments.foldIsShared(argName, pinned)) {
      "${field.name} (argument '$argName' matches more than one passed argument differing only " +
        "in case or underscores; add @ArgName)"
    } else {
      "${field.name} (argument '$argName')"
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
   * cross-language fold, and tracking which of them a field has taken. A fold
   * two arguments share matches neither: either could be the one meant, and
   * handing a field the wrong value is worse than failing.
   */
  private class ArgIndex(
    private val bindings: List<ArgBinding>,
  ) {
    private val byName = bindings.associateBy { it.name }
    private val byFold = mutableMapOf<String, ArgBinding>()
    private val sharedFolds = mutableSetOf<String>()
    private val claimed = mutableSetOf<String>()

    init {
      bindings.forEach { binding ->
        val fold = foldArgName(binding.name)
        if (byFold.put(fold, binding) != null) sharedFolds += fold
      }
    }

    /**
     * Takes the argument a field claims, marking it claimed. An
     * `@ArgName`-pinned name is taken literally, which is what makes the
     * annotation an escape hatch for a Python name no Java identifier folds
     * to.
     */
    fun claim(
      name: String,
      pinned: Boolean,
    ): ArgBinding? {
      val match = byName[name] ?: foldMatch(name, pinned)
      return match?.also { claimed += it.name }
    }

    private fun foldMatch(
      name: String,
      pinned: Boolean,
    ): ArgBinding? {
      if (pinned) return null
      val fold = foldArgName(name)
      return if (fold in sharedFolds) null else byFold[fold]
    }

    /** Whether [name] reaches two arguments the fold cannot tell apart. */
    fun foldIsShared(
      name: String,
      pinned: Boolean,
    ): Boolean = !pinned && name !in byName && foldArgName(name) in sharedFolds

    /** Explicitly passed argument names no field took. */
    fun unclaimed(): List<String> = bindings.filterNot { it.fromDefault || it.name in claimed }.map { it.name }

    /** Every argument name the call site passed, captured defaults included. */
    fun passed(): List<String> = bindings.map { it.name }
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
