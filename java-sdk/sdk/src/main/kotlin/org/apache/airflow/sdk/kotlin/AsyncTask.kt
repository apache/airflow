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

package org.apache.airflow.sdk.kotlin

import org.apache.airflow.sdk.Builder
import org.apache.airflow.sdk.Context
import kotlin.Throws

/**
 * A coroutine-based unit of work executed by Airflow.
 *
 * Prefer annotating a suspending function with [Builder.Task] so the annotation
 * processor generates this implementation. Implement the interface directly
 * only for low-level plumbing.
 */
interface AsyncTask {
  /** Executes this task without blocking its thread during Airflow API calls. */
  @Throws(Exception::class)
  suspend fun execute(
    context: Context,
    client: AsyncClient,
  )
}
