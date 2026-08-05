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

import org.apache.airflow.sdk.internal.ArgValues

/**
 * A task's arguments addressed **by position**, for an [InputTask] whose call
 * site is not worth a dedicated [TaskInput] bundle class.
 *
 * ```java
 * public class Transform implements InputTask<TaskArgs> {
 *   @Override
 *   public void execute(Context context, Client client, TaskArgs args) {
 *     long extracted = args.require(0, Long.class);
 *   }
 * }
 * ```
 *
 * Positions follow the stub signature's declaration order, so a keyword call
 * in the Dag file addresses the same positions as a positional one.
 *
 * @see InputTask
 * @see TaskInput
 */
class TaskArgs internal constructor(
  private val context: Context,
  private val client: Client,
) : TaskInput {
  /** How many arguments the `@task.stub` call site bound. */
  fun size(): Int = client.argBindings.size

  /**
   * Resolves the argument bound at [position] into [type], passing null
   * through.
   *
   * @param position Zero-based position in the stub signature's argument list.
   * @param type Type to decode the bound value into.
   * @return The bound value, or `null` when the binding resolves to nothing.
   * @throws IllegalStateException if the call site bound no argument at
   *    [position].
   * @throws ApiError if the underlying XCom read fails.
   */
  fun <T : Any> get(
    position: Int,
    type: Class<T>,
  ): T? = ArgValues.optionalInput(context, client, position, type)

  /**
   * Resolves the argument bound at [position] into [type], which must not be
   * null.
   *
   * @param position Zero-based position in the stub signature's argument list.
   * @param type Type to decode the bound value into.
   * @return The bound value.
   * @throws IllegalStateException if the call site bound no argument at
   *    [position].
   * @throws MissingXComException if the binding resolves to nothing — a null
   *    literal, or an upstream that pushed no XCom.
   * @throws ApiError if the underlying XCom read fails.
   */
  fun <T : Any> require(
    position: Int,
    type: Class<T>,
  ): T =
    get(position, type) ?: throw MissingXComException(
      "Argument '${argNameAt(position)}' of task '${client.details.ti.taskId}' resolved to nothing; " +
        "read it with get() to accept null.",
    )

  // The stub signature's own parameter name is the clearest label for a failure
  // at this position; nothing bound there fails in get() with the arity
  // mismatch instead, so the fallback is only ever a formatting guard.
  private fun argNameAt(position: Int): String = client.argBindings.getOrNull(position)?.name ?: "position $position"
}
