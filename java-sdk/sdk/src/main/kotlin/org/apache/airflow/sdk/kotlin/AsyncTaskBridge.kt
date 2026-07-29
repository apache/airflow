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

import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.startCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

/** @suppress */
fun interface SuspendTaskCall<T> {
  fun invoke(
    xcomValues: List<Any?>,
    continuation: Continuation<T>,
  ): Any?
}

/**
 * Runtime bridge used by Java sources generated for Kotlin suspending tasks.
 *
 * @suppress
 */
object AsyncTaskBridge {
  @JvmStatic
  fun <T> execute(
    client: AsyncClient,
    xcomTaskIds: Array<String>,
    publishResult: Boolean,
    call: SuspendTaskCall<T>,
    completion: Continuation<Unit>,
  ): Any? =
    suspend {
      val xcomValues = xcomTaskIds.map { client.getXCom(taskId = it) }
      val result =
        suspendCoroutineUninterceptedOrReturn<T> { continuation ->
          call.invoke(xcomValues, continuation)
        }
      if (publishResult) {
        client.setXCom(value = result as Any)
      }
    }.startCoroutineUninterceptedOrReturn(completion)
}
