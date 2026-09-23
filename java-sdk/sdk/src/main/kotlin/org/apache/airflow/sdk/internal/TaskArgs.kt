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

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.MissingXComException

/**
 * @suppress
 *
 * A task's arguments addressed **by position**, in the stub signature's
 * declaration order.
 *
 * This is what the annotation processor emits for a task method's flat data
 * parameters, and it is not user-facing API: positional access is safe in code
 * the processor writes and type-checks against the method signature, and wrong
 * to ask a Dag author to track by hand. A task written against the interface
 * declares an [org.apache.airflow.sdk.TaskInput] instead.
 *
 * ```java
 * TaskArgs args = TaskArgs.of(context, client, 2);
 * long rows = args.require(0, Long.class);
 * List<String> regions = args.get(1, new TypeRef<List<String>>() {});
 * ```
 */
class TaskArgs private constructor(
  private val context: Context,
  private val client: Client,
) {
  companion object {
    /**
     * Opens a positional view over the arguments bound for this run, for a
     * task method declaring [declared] data parameters.
     *
     * The two counts must match exactly. Positions carry the whole meaning of
     * a flat binding, so a call site that bound a different number of
     * arguments than the method takes has already shifted them: too few leaves
     * a parameter unbound, and too many means the extras — or the ones ahead
     * of them — are not the arguments the method believes it is reading.
     *
     * @throws IllegalStateException if the call site bound a different number
     *    of arguments than the task declares.
     */
    @JvmStatic
    fun of(
      context: Context,
      client: Client,
      declared: Int,
    ): TaskArgs {
      val wired = ArgValues.wiredInputs(context, client)
      val supplied = wired?.size ?: client.argBindings.size
      val source = if (wired != null) "Dag wired" else "stub call bound"
      check(supplied == declared) {
        "Task '${context.ti.taskId}' declares $declared data parameter(s) " +
          "but the $source $supplied argument(s)"
      }
      return TaskArgs(context, client)
    }
  }

  /**
   * Resolves the argument bound at [position] into [type], passing null
   * through.
   *
   * @throws org.apache.airflow.sdk.ApiError if the underlying XCom read fails.
   */
  fun <T : Any> get(
    position: Int,
    type: Class<T>,
  ): T? = type.cast(ArgValues.valueAt(context, client, position, type))

  /**
   * Resolves the argument bound at [position] into the generic [type], passing
   * null through.
   *
   * @throws org.apache.airflow.sdk.ApiError if the underlying XCom read fails.
   */
  @Suppress("UNCHECKED_CAST")
  fun <T : Any> get(
    position: Int,
    type: TypeRef<T>,
  ): T? = ArgValues.valueAt(context, client, position, type.type) as T?

  /**
   * Resolves the argument bound at [position] into [type], which must not be
   * null.
   *
   * @throws MissingXComException if the binding resolves to nothing — a null
   *    literal, or an upstream that pushed no XCom.
   * @throws org.apache.airflow.sdk.ApiError if the underlying XCom read fails.
   */
  fun <T : Any> require(
    position: Int,
    type: Class<T>,
  ): T = get(position, type) ?: throw missingAt(position)

  /**
   * Resolves the argument bound at [position] into the generic [type], which
   * must not be null.
   *
   * @throws MissingXComException if the binding resolves to nothing — a null
   *    literal, or an upstream that pushed no XCom.
   * @throws org.apache.airflow.sdk.ApiError if the underlying XCom read fails.
   */
  fun <T : Any> require(
    position: Int,
    type: TypeRef<T>,
  ): T = get(position, type) ?: throw missingAt(position)

  // A bound argument is labelled with the stub signature's own parameter name,
  // which is what the Dag author has to change. A wired one has no such name,
  // so it is labelled with the position it feeds.
  private fun missingAt(at: Int): MissingXComException {
    val wired = ArgValues.wiredInputs(context, client)
    return if (wired != null) {
      ArgValues.missingWired(wired[at], "position $at")
    } else {
      ArgValues.missing(client.argBindings[at], context.ti.taskId)
    }
  }
}
