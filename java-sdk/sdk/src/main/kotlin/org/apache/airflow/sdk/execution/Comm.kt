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
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.msgpack.core.buffer.MessageBuffer
import org.msgpack.core.buffer.MessageBufferInput
import java.io.IOException
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * A [MessageBufferInput] that feeds a MessageUnpacker in chunks.
 *
 * Up to [CHUNK_SIZE] bytes are read from [reader] per chunk, [declaredLength]
 * bytes in total. This bounds only the transport read buffer so a frame larger
 * than [Int.MAX_VALUE] can decode without one giant allocation.
 *
 * The MessageBufferInput contract is synchronous while the underlying read
 * suspends, so each [next] bridges with [runBlocking]. This is fine since we
 * only use this class with `Dispatchers.IO`, which is capable of blocking.
 */
private class ChannelFrameInput(
  private val reader: ByteReadChannel,
  declaredLength: UInt,
  private val coroutineContext: CoroutineContext,
) : MessageBufferInput {
  private companion object {
    const val CHUNK_SIZE = (64 * 1024).toLong()
  }

  private var remaining = declaredLength.toLong()

  override fun next(): MessageBuffer? {
    if (remaining <= 0L) return null
    coroutineContext.ensureActive()
    val array =
      minOf(remaining, CHUNK_SIZE).toInt().let { want ->
        runBlocking(coroutineContext[Job] ?: EmptyCoroutineContext) {
          reader.readByteArray(want)
        }.apply {
          if (size != want) throw IOException("Truncated frame: expected $want more bytes, got $size")
          remaining -= want
        }
      }
    return MessageBuffer.wrap(array)
  }

  override fun close() {} // No cleanup here. The caller owns the channel's lifecycle.
}

data class IncomingFrame(
  val id: Int,
  val body: Any?,
)

@OptIn(ExperimentalAtomicApi::class)
class CoordinatorComm(
  private val bundle: Bundle,
  private val reader: ByteReadChannel,
  private val writer: ByteWriteChannel,
) {
  internal companion object {
    private val logger = Logger(CoordinatorComm::class)
  }

  private val nextId = AtomicInt(0)
  private var shutDownRequested = false

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

    val declaredLength = Frame.parseLengthPrefix(prefix)
    val frame =
      try {
        Frame.decode(ChannelFrameInput(reader, declaredLength, currentCoroutineContext()))
      } catch (e: CancellationException) {
        throw e // Let coroutine cancellation propagate so the task coroutine unwinds.
      } catch (e: Exception) {
        logger.error(
          "Failed to read or decode frame",
          mapOf("length" to declaredLength, "exception" to e),
        )
        shutDownRequested = true
        return
      }
    logger.debug("Handling", mapOf("id" to frame.id))
    handle(frame)
  }

  private suspend fun sendMessage(
    id: Int,
    body: Any,
  ) {
    val buffers = Frame.encodeRequest(id, body)
    logger.debug("Sending", mapOf("id" to id, "body" to body))
    writer.writeByteArray(Frame.lengthPrefix(Frame.payloadLength(buffers)))
    for (buffer in buffers) {
      writer.writeByteArray(buffer.toByteArray())
    }
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
    var frame: IncomingFrame? = null

    suspend fun handle(f: IncomingFrame) {
      frame = f
    }
    sendMessage(nextId.fetchAndAdd(1), body)
    processOnce(::handle)
    if (frame == null) {
      throw ApiError("No response received")
    }
    return frame.body ?: Unit
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
