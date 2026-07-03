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

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

data class IncomingFrame(
  val id: Int,
  val body: Any?,
)

data class OutgoingFrame(
  val id: Int,
  val body: Any,
)

@OptIn(ExperimentalAtomicApi::class)
class CoordinatorComm(
  private val reader: ByteReadChannel,
  private val writer: ByteWriteChannel,
) : AutoCloseable {
  internal companion object {
    private val logger = Logger(CoordinatorComm::class)

    fun encode(outgoing: OutgoingFrame) = Frame.encodeRequest(outgoing.id, outgoing.body)

    fun decode(bytes: ByteArray) = Frame.decode(bytes)
  }

  private val nextId = AtomicInt(0)
  private val writeMutex = Mutex()
  private val stateMutex = Mutex()
  private val pending = mutableMapOf<Int, CompletableDeferred<IncomingFrame>>()
  private var dispatcherStarted = false
  private var readError: ApiError? = null

  private val dispatcherScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

  suspend fun readMessage(): IncomingFrame =
    stateMutex.withLock {
      check(!dispatcherStarted) { "readMessage cannot be used after the dispatcher has started" }
      readFrame()
    }

  private suspend fun sendMessage(
    id: Int,
    body: Any,
  ) {
    val data = encode(OutgoingFrame(id, body))
    logger.debug("Sending", mapOf("id" to id, "body" to body))
    writeMutex.withLock {
      writer.writeByteArray(Frame.lengthPrefix(data.size))
      writer.writeByteArray(data)
    }
  }

  @Throws(ApiError::class)
  suspend fun communicateImpl(body: Any): Any {
    val requestId = nextId.fetchAndAdd(1)
    val waiter = CompletableDeferred<IncomingFrame>()

    stateMutex.withLock {
      readError?.let { throw it }
      if (!dispatcherStarted) {
        dispatcherStarted = true
        dispatcherScope.launch { readLoop() }
      }
      pending[requestId] = waiter
    }

    try {
      sendMessage(requestId, body)
    } catch (e: Throwable) {
      stateMutex.withLock { pending.remove(requestId) }
      throw e
    }

    return waiter.await().body ?: Unit
  }

  @Throws(ApiError::class)
  suspend inline fun <reified T> communicate(request: Any): T {
    when (val response = communicateImpl(request)) {
      is ErrorResponse -> throw ApiError("[${response.error}] ${response.detail}")
      is T -> return response
      else -> throw ApiError("Unexpected response type ${response::class.java}")
    }
  }

  override fun close() {
    dispatcherScope.cancel()
  }

  private suspend fun readFrame(): IncomingFrame {
    val prefix = reader.readByteArray(4) // First 4 bytes as length.
    if (prefix.size != 4) {
      throw ApiError("Coordinator socket closed while reading frame length")
    }
    val payloadLength = Frame.parseLengthPrefix(prefix)
    val payload = reader.readByteArray(payloadLength)
    if (payload.size != payloadLength) {
      throw ApiError("Coordinator socket closed while reading frame payload")
    }
    val frame = decode(payload)
    logger.debug("Received", mapOf("id" to frame.id))
    return frame
  }

  private suspend fun readLoop() {
    while (true) {
      val frame =
        try {
          readFrame()
        } catch (e: CancellationException) {
          // Coroutine cancellation is delivered by throwing this exception, and
          // cooperative cancellation requires rethrowing it so it propagates.
          throw e
        } catch (e: Throwable) {
          failAllWaiters(e)
          return
        }
      val waiter = stateMutex.withLock { pending.remove(frame.id) }
      if (waiter == null) {
        logger.warning("Discarding response with no matching request", mapOf("id" to frame.id))
        continue
      }
      waiter.complete(frame)
    }
  }

  private suspend fun failAllWaiters(cause: Throwable) {
    val error = cause as? ApiError ?: ApiError("Coordinator comm closed: ${cause.message}")
    val waiters =
      stateMutex.withLock {
        readError = error
        val snapshot = pending.values.toList()
        pending.clear()
        snapshot
      }
    waiters.forEach { it.completeExceptionally(error) }
  }
}
