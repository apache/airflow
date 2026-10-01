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

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.InputTask
import org.apache.airflow.sdk.Task
import org.apache.airflow.sdk.TaskDef
import org.apache.airflow.sdk.TaskInput
import org.apache.airflow.sdk.execution.comm.TaskHandlerDeclaration.Binding
import org.apache.airflow.sdk.execution.comm.TaskHandlerParseRequest
import org.apache.airflow.sdk.internal.TaskParams
import org.apache.airflow.sdk.internal.argNameOf
import org.apache.airflow.sdk.internal.bindableFields
import org.apache.airflow.sdk.internal.buildValueSchema
import org.apache.airflow.sdk.internal.isPinned
import org.apache.airflow.sdk.internal.resolveInputType
import java.lang.reflect.Modifier
import java.lang.reflect.Type

/**
 * Answers a [TaskHandlerParseRequest] with every task handler [bundle]
 * registers, keyed by Dag id in registration order, as a
 * TaskHandlerParsingResult body.
 *
 * Only task-handler registrations count: a Dag declared in Java is its own,
 * not a Python Dag's. The generated models cannot hold `task_handlers`, so
 * the body is a map.
 */
internal fun parseTaskHandlers(
  bundle: Bundle,
  request: TaskHandlerParseRequest,
): Map<String, Any?> =
  linkedMapOf(
    "type" to "TaskHandlerParsingResult",
    "fileloc" to request.file,
    "task_handlers" to bundle.taskHandlers.mapValues { (_, dag) -> dag.tasks.values.map(::declare) },
  )

private fun declare(task: TaskDef): Map<String, Any?> {
  val (binding, params) = bindingOf(task.definition)
  return linkedMapOf(
    "task_id" to task.id,
    "binding" to binding.value(),
    "params" to params,
  )
}

/**
 * How [definition] binds the stub call's arguments. Flat parameters bind by
 * position; a [TaskInput] binds by name. A task that reads no argument is
 * declared as binding by name with no parameters: it ignores an argument
 * passed to it, so the Dag processor only warns about one.
 */
private fun bindingOf(definition: Class<out Task>): Pair<Binding, List<Map<String, Any?>>> {
  if (InputTask::class.java.isAssignableFrom(definition)) {
    return Binding.NAMED to inputParams(resolveInputType(definition))
  }
  val declared = taskParamsOf(definition) ?: return Binding.NAMED to emptyList()
  declared.input?.let { return Binding.NAMED to inputParams(it) }
  if (declared.flat.isEmpty()) return Binding.NAMED to emptyList()
  return Binding.POSITIONAL to declared.flat.map { param(it.name, it.type, exactName = false) }
}

/** The [TaskParams] the annotation processor recorded on a generated task class, if [definition] is one. */
private fun taskParamsOf(definition: Class<*>): TaskParams? {
  val field =
    try {
      definition.getField(TaskParams.FIELD)
    } catch (_: NoSuchFieldException) {
      return null
    }
  return if (Modifier.isStatic(field.modifiers)) field.get(null) as? TaskParams else null
}

private fun inputParams(inputType: Class<out TaskInput>): List<Map<String, Any?>> =
  bindableFields(inputType).map { param(argNameOf(it), it.genericType, exactName = isPinned(it)) }

private fun param(
  name: String,
  type: Type,
  exactName: Boolean,
): Map<String, Any?> =
  linkedMapOf<String, Any?>(
    "name" to name,
    "exact_name" to exactName,
  ).apply { buildValueSchema(type)?.let { put("value_schema", it) } }
