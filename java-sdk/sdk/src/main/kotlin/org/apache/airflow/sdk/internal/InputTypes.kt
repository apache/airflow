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

import org.apache.airflow.sdk.InputTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskArgs
import org.apache.airflow.sdk.TaskInput
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

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
 * bundles that `@Builder.Task` methods declare.
 *
 * @throws IllegalArgumentException if the declared input type is unresolvable
 *    or cannot be populated.
 */
internal fun validateTaskInput(definition: Class<out Task>) {
  if (!InputTask::class.java.isAssignableFrom(definition)) return
  val inputType = resolveInputType(definition)
  // TaskArgs reads the bindings positionally instead of holding fields, so the
  // SDK constructs it rather than the user.
  if (inputType == TaskArgs::class.java) return
  requirePublicNoArgConstructor(inputType)
  bindableFields(inputType)
}

/**
 * Every field of a [TaskInput] bundle that binds an argument: each public
 * non-final instance field, its own and inherited.
 *
 * @throws IllegalArgumentException if any instance field cannot be assigned,
 *    which would leave an argument silently unbound.
 */
internal fun bindableFields(inputType: Class<*>): List<Field> {
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
        fields += field
      }
    current = current.superclass
  }
  return fields
}

/** Instantiates a [TaskInput] bundle for the SDK to populate. */
internal fun <I : TaskInput> newInput(inputType: Class<I>): I {
  requirePublicNoArgConstructor(inputType)
  // A bundle class may be package-private even though its constructor and
  // fields are public, which reflection from the SDK needs opening.
  return inputType.getDeclaredConstructor().also { it.isAccessible = true }.newInstance()
}

private fun requirePublicNoArgConstructor(inputType: Class<*>) {
  val constructor = inputType.declaredConstructors.firstOrNull { it.parameterCount == 0 }
  require(constructor != null && Modifier.isPublic(constructor.modifiers)) {
    "TaskInput class ${inputType.simpleName} needs a public no-argument constructor"
  }
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
