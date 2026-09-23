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
 * One stub-task argument bound at the `@task.stub` TaskFlow call site in the
 * Python Dag file, delivered via `TIRunContext.arg_bindings`.
 *
 * The supervisor schema models this as a `kind`-discriminated union
 * (`XComArgBinding` / `LiteralArgBinding`), which jsonSchema2Pojo cannot
 * express as a typed field — the generated `TIRunContext.argBindings` is a
 * plain `Object` holding the msgpack-decoded list of maps — so this hand-
 * written decoder materializes the typed view.
 */
internal sealed class ArgBinding {
  abstract val name: String

  internal data class XCom(
    override val name: String,
    val taskId: String,
    val mapIndex: Int,
    val elementIndex: Int?,
  ) : ArgBinding()

  internal data class Literal(
    override val name: String,
    val value: Any?,
  ) : ArgBinding()
}

/**
 * Decodes the raw `TIRunContext.argBindings` payload into a list of bindings
 * preserving the stub signature's parameter order — flat task parameters
 * bind by that position, input-bundle fields by [ArgBinding.name].
 *
 * @throws IllegalStateException on a malformed payload, an unsupported
 *    binding kind, or a duplicate argument name; the task cannot bind its
 *    arguments correctly, so it must fail rather than run with wrong inputs.
 */
internal fun decodeArgBindings(raw: Any?): List<ArgBinding> {
  if (raw == null) return emptyList()
  check(raw is List<*>) { "arg_bindings payload is not a list: ${raw.javaClass.name}" }
  val seen = mutableSetOf<String>()
  return raw.map { entry ->
    check(entry is Map<*, *>) { "arg_bindings entry is not a map: $entry" }
    val name = checkNotNull(entry["name"] as? String) { "arg_bindings entry has no name: $entry" }
    check(seen.add(name)) { "arg_bindings entries have duplicate name: '$name'" }
    when (val kind = entry["kind"]) {
      "literal" -> ArgBinding.Literal(name = name, value = entry["value"])
      "xcom" ->
        ArgBinding.XCom(
          name = name,
          taskId = checkNotNull(entry["task_id"] as? String) { "xcom arg binding '$name' has no task_id" },
          mapIndex = (entry["map_index"] as? Number)?.toInt() ?: -1,
          elementIndex = (entry["element_index"] as? Number)?.toInt(),
        )
      else -> error("Unsupported arg binding kind '$kind' for argument '$name'")
    }
  }
}
