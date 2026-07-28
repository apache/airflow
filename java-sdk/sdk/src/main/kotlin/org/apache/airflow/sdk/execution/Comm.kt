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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import org.apache.airflow.sdk.execution.comm.StartupDetails
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
  private val bundle: Bundle,
  private val reader: ByteReadChannel,
  private val writer: ByteWriteChannel,
) {
  internal companion object {
    private val logger = Logger(CoordinatorComm::class)

    fun encode(outgoing: OutgoingFrame) = Frame.encodeRequest(outgoing.id, outgoing.body)

    fun decode(bytes: ByteArray) = Frame.decode(bytes)
  }

  private val nextId = AtomicInt(0)
  private var shutDownRequested = false
  private val commMutex = Mutex()

  suspend fun startProcessing() {
    while (!shutDownRequested) {
      processOnce(::handleIncoming)
    }
    logger.debug("Goodbye")
  }

  private suspend fun processOnce(handle: suspend (IncomingFrame) -> Unit) {
    val prefix = reader.readByteArray(4) // First 4 bytes as length.
    if (prefix.size != 4) { // Something is terribly wrong. Let's bail.
      logger.error("Need 4 prefix bytes", mapOf("actual" to prefix.size))
      shutDownRequested = true
      return
    }

    val payloadLength = Frame.parseLengthPrefix(prefix)
    val payload = reader.readByteArray(payloadLength)
    if (payload.size != payloadLength) { // Something is terribly wrong. Let's bail.
      logger.error(
        "Payload length not right",
        mapOf("expect" to payloadLength, "receive" to payload.size),
      )
      shutDownRequested = true
      return
    }
    val frame = decode(payload)
    logger.debug("Handling", mapOf("id" to frame.id))
    handle(frame)
  }

  private suspend fun sendMessage(
    id: Int,
    body: Any,
  ) {
    val data = encode(OutgoingFrame(id, body))
    logger.debug("Sending", mapOf("id" to id, "body" to body))
    writer.writeByteArray(Frame.lengthPrefix(data.size))
    writer.writeByteArray(data)
  }

  suspend fun handleIncoming(frame: IncomingFrame) {
    when (val request = frame.body) {
      null -> {}
      is ErrorResponse -> throw ApiError("[${request.error}] ${request.detail}")
      is StartupDetails -> {
        communicate<Unit>(runTask(bundle, request, this))
        shutDownRequested = true
      }
    }
  }

  @Throws(ApiError::class)
  suspend fun communicateImpl(body: Any): Any {
    val requestId = nextId.fetchAndAdd(1)
    return commMutex.withLock {
      var frame: IncomingFrame? = null

      suspend fun handle(f: IncomingFrame) {
        frame = f
      }
      sendMessage(requestId, body)
      processOnce(::handle)
      val received = frame ?: throw ApiError("No response received")
      if (received.id != requestId) {
        throw ApiError("response id ${received.id} does not match request id $requestId")
      }
      received.body ?: Unit
    }
  }

  @Throws(ApiError::class)
  suspend inline fun <reified T> communicate(request: Any): T {
    when (val response = communicateImpl(request)) {
      is ErrorResponse -> throw ApiError("[${response.error}] ${response.detail}")
      is T -> return response
      else -> throw ApiError("Unexpected response type ${response::class.java}")
    }
  }
}
