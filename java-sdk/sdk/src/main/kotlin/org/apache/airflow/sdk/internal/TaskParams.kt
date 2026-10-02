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

import org.apache.airflow.sdk.TaskInput
import java.lang.reflect.Type

/**
 * @suppress
 *
 * The data parameters of a task class generated for a `@Builder.TaskHandler`
 * method, as the runtime reports them when asked which task handlers a bundle
 * registers. The generated class holds it in a static field named [FIELD].
 *
 * Java parameter names do not survive compilation, so the annotation
 * processor records them here. Public so that generated code can build it;
 * not user-facing API.
 */
class TaskParams private constructor(
  internal val flat: List<Param>,
  internal val input: Class<out TaskInput>?,
) {
  /** One flat data parameter: its name in the handler method, and its declared type. */
  class Param internal constructor(
    internal val name: String,
    internal val type: Type,
  )

  companion object {
    /** Name of the static field holding a generated task class's [TaskParams]. */
    const val FIELD = "AIRFLOW_TASK_PARAMS"

    /** Flat data parameters, in declaration order; none for a method that takes no data. */
    @JvmStatic
    fun of(vararg params: Param): TaskParams = TaskParams(params.toList(), null)

    /** A single [TaskInput] parameter of [type]. */
    @JvmStatic
    fun input(type: Class<out TaskInput>): TaskParams = TaskParams(emptyList(), type)

    /** A flat parameter whose declared type a `Class` literal expresses; primitives stay primitive. */
    @JvmStatic
    fun param(
      name: String,
      type: Class<*>,
    ): Param = Param(name, type)

    /** A flat parameter whose declared type has type arguments. */
    @JvmStatic
    fun param(
      name: String,
      type: TypeRef<*>,
    ): Param = Param(name, type.type)
  }
}
