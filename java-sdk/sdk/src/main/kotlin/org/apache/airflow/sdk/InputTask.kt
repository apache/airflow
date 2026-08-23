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
import org.apache.airflow.sdk.internal.resolveInputType
import kotlin.Throws

/**
 * A [Task] whose input the SDK resolves from the `@task.stub` TaskFlow call
 * site in the Python Dag file and injects.
 *
 * The type argument declares what the task expects, and it is the only place
 * that declaration lives:
 *
 * - a [TaskInput] bundle binds each of its public fields **by name**, the
 *   tagged boundary where the stub's `snake_case` argument names cross into
 *   `camelCase` Java fields;
 * - [TaskArgs] reads the same arguments **by position**, for a call site not
 *   worth a dedicated class.
 *
 * ```java
 * public class Summarize implements InputTask<SummarizeInput> {
 *   @Override
 *   public void execute(Context context, Client client, SummarizeInput input) {
 *     // input.region, input.transformed
 *   }
 * }
 * ```
 *
 * Plain [Task] stays the right choice for a task the Dag file calls with no
 * arguments.
 *
 * @param I Type of this task's input: a [TaskInput] bundle, or [TaskArgs].
 *
 * @see TaskInput
 * @see TaskArgs
 */
interface InputTask<I : TaskInput> : Task {
  /**
   * Resolves this task's declared input, then runs [execute]. Implementations
   * override the three-argument [execute] instead of this method.
   */
  @Throws(Exception::class)
  override fun execute(
    context: Context,
    client: Client,
  ) = execute(context, client, ArgValues.bindInput(context, client, inputType()))

  /**
   * Executes this task.
   *
   * Any exception thrown marks the task instance as failed. Use [client] to
   * read connections, variables, pull XComs, or to push an XCom for downstream
   * tasks.
   *
   * @param context Runtime context for the current execution workload.
   * @param client Client for Airflow API calls scoped to this execution.
   * @param input This task's arguments, as bound at the stub call site.
   * @throws Exception on failure; the task instance is marked failed.
   */
  @Throws(Exception::class)
  fun execute(
    context: Context,
    client: Client,
    input: I,
  )
}

/**
 * Recovers the [TaskInput] type this task bound to [InputTask]'s type
 * parameter. [TaskDef] resolves it up front, so reaching a task run means it
 * is resolvable.
 */
@Suppress("UNCHECKED_CAST")
private fun <I : TaskInput> InputTask<I>.inputType(): Class<I> = resolveInputType(javaClass) as Class<I>
