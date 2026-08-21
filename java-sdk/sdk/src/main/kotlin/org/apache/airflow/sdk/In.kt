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

package org.apache.airflow.sdk

/**
 * One data input of a task in a [Wiring] method: either the output of an
 * upstream task (a [TaskRef] returned by another flow-twin call) or an
 * inline literal created with [In.value].
 *
 * ```java
 * f.transform(f.extract());          // upstream output
 * f.score(In.value(0.5));            // inline literal
 * ```
 *
 * @param T Declared type of the task parameter this input feeds.
 */
sealed class In<T> {
  companion object {
    /**
     * Wraps an inline literal as a task input.
     *
     * The value is delivered to the task parameter as-is; numeric values
     * widen to the parameter's declared numeric type.
     *
     * @param value Literal to bind; may be null for nullable parameters.
     */
    @JvmStatic
    fun <T> value(value: T?): In<T> = LiteralIn(value)
  }
}

internal class LiteralIn<T>(
  internal val value: T?,
) : In<T>()

/**
 * The output of a task registered by a flow-twin call in a [Wiring] method.
 *
 * Passing a handle to another twin call feeds this task's return value into
 * that task's parameter and wires the dependency edge — the calls in the
 * wiring method are the single way to declare dependencies.
 *
 * @param T Return type of the task this handle refers to.
 */
class TaskRef<T> internal constructor(
  internal val def: TaskDef,
) : In<T>()
