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

import org.apache.airflow.sdk.ArgName
import org.apache.airflow.sdk.InputTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskInput
import java.lang.reflect.Constructor
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.concurrent.ConcurrentHashMap

/**
 * Resolves the [TaskInput] type argument that [taskClass] bound to
 * [InputTask]'s type parameter.
 *
 * @throws IllegalArgumentException if the type argument is not a concrete
 *    [TaskInput] class.
 */
internal fun resolveInputType(taskClass: Class<*>): Class<out TaskInput> =
  findInputType(taskClass)
    ?: throw IllegalArgumentException(
      "Task class ${taskClass.name} implements InputTask with an input type that cannot be resolved; " +
        "declare a concrete type argument, e.g. 'implements InputTask<MyInput>'",
    )

/**
 * Checks that an [InputTask]'s declared input can be bound, before the task
 * ever runs — a mis-declared input then fails while the [org.apache.airflow.sdk.Bundle]
 * is being built rather than mid-run. A plain [Task] declares no input and passes.
 *
 * The annotation processor enforces the same rules at compile time for the
 * [TaskInput] a `@Builder.Task` method declares.
 *
 * @throws IllegalArgumentException if the declared input type is unresolvable
 *    or cannot be populated.
 */
internal fun validateTaskInput(definition: Class<out Task>) {
  if (!InputTask::class.java.isAssignableFrom(definition)) return
  val inputType = resolveInputType(definition)
  requirePublicNoArgConstructor(inputType)
  bindableFields(inputType)
}

/**
 * Every field of a [TaskInput] that binds an argument: each public non-final
 * instance field, its own and inherited.
 *
 * Memoized, since the answer is fixed by the class: a worker binds the same
 * input once per task instance it runs, and reflecting over the whole
 * hierarchy each time buys nothing.
 *
 * @throws IllegalArgumentException if any instance field cannot be assigned,
 *    which would leave an argument silently unbound, or if two fields claim
 *    argument names that [foldArgName] cannot tell apart.
 */
internal fun bindableFields(inputType: Class<*>): List<Field> =
  bindableFieldsByType.computeIfAbsent(inputType) { collectBindableFields(it) }

private val bindableFieldsByType = ConcurrentHashMap<Class<*>, List<Field>>()

private fun collectBindableFields(inputType: Class<*>): List<Field> {
  val fields = mutableListOf<Field>()
  var current: Class<*>? = inputType
  while (current != null && current != Any::class.java) {
    current.declaredFields
      .filterNot { Modifier.isStatic(it.modifiers) || it.isSynthetic }
      .forEach { field ->
        require(Modifier.isPublic(field.modifiers) && !Modifier.isFinal(field.modifiers)) {
          "TaskInput field ${inputType.simpleName}.${field.name} must be public and non-final " +
            "so the SDK can assign its binding"
        }
        // The declaring class may be package-private even though the field is
        // public, which reflection from the SDK needs opening.
        field.isAccessible = true
        fields += field
      }
    current = current.superclass
  }
  requireDistinctArgNames(inputType, fields)
  return fields
}

/** The argument name a field claims: its [ArgName] value, or its own name. */
internal fun argNameOf(field: Field): String = field.getAnnotation(ArgName::class.java)?.value ?: field.name

/** Whether [ArgName] pinned this field's argument name, which forbids the fold. */
internal fun isPinned(field: Field): Boolean = field.isAnnotationPresent(ArgName::class.java)

/**
 * @suppress
 *
 * Reduces an argument name to the token that matches across languages —
 * lowercased with underscores removed, the same rule the Go and TypeScript
 * SDKs fold by, so one Python signature binds identically in all three.
 *
 * Public so the annotation processor can reject a clash at compile time with
 * the same rule the runtime binds by; not user-facing API.
 */
fun foldArgName(name: String): String = name.replace("_", "").lowercase()

private fun requireDistinctArgNames(
  inputType: Class<*>,
  fields: List<Field>,
) {
  val claimed = mutableMapOf<String, String>()
  fields.forEach { field ->
    val previous = claimed.put(foldArgName(argNameOf(field)), field.name)
    require(previous == null) {
      "TaskInput fields ${inputType.simpleName}.$previous and ${inputType.simpleName}.${field.name} " +
        "claim argument names that differ only in case or underscores, which the fold cannot tell " +
        "apart; rename one of them"
    }
  }
}

/** Instantiates a [TaskInput] for the SDK to populate. */
@Suppress("UNCHECKED_CAST")
internal fun <I : TaskInput> newInput(inputType: Class<I>): I = requirePublicNoArgConstructor(inputType).newInstance() as I

/** Memoized alongside [bindableFields], and for the same reason. */
private val constructorsByType = ConcurrentHashMap<Class<*>, Constructor<*>>()

private fun requirePublicNoArgConstructor(inputType: Class<*>): Constructor<*> =
  constructorsByType.computeIfAbsent(inputType) {
    val constructor = it.declaredConstructors.firstOrNull { c -> c.parameterCount == 0 }
    require(constructor != null && Modifier.isPublic(constructor.modifiers)) {
      "TaskInput class ${it.simpleName} needs a public no-argument constructor"
    }
    // The class may be package-private even though its constructor is public,
    // which reflection from the SDK needs opening.
    constructor.also { c -> c.isAccessible = true }
  }

/**
 * Walks [type]'s supertypes for the [InputTask] type argument. A type variable
 * yields null: only the class that fixes it to a concrete [TaskInput] can say
 * what to bind.
 */
private fun findInputType(type: Type?): Class<out TaskInput>? =
  when (type) {
    is ParameterizedType ->
      if (type.rawType == InputTask::class.java) {
        type.actualTypeArguments.firstOrNull().asTaskInputClass()
      } else {
        findInputType(type.rawType)
      }
    is Class<*> ->
      (type.genericInterfaces.asSequence() + sequenceOf(type.genericSuperclass))
        .firstNotNullOfOrNull { findInputType(it) }
    else -> null
  }

@Suppress("UNCHECKED_CAST")
private fun Type?.asTaskInputClass(): Class<out TaskInput>? =
  (this as? Class<*>)?.takeIf { TaskInput::class.java.isAssignableFrom(it) } as Class<out TaskInput>?
