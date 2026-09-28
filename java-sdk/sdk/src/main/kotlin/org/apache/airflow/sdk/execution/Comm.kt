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
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import org.msgpack.core.buffer.MessageBuffer
import org.msgpack.core.buffer.MessageBufferInput
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
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

  var remaining = declaredLength.toLong()
    private set

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

data class OutgoingFrame(
  val id: Int,
  val body: Any,
)

internal class FrameProcessingException(
  message: String,
  cause: Throwable? = null,
) : Exception(message, cause)

@OptIn(ExperimentalAtomicApi::class)
class CoordinatorComm(
  private val reader: ByteReadChannel,
  private val writer: ByteWriteChannel,
) : AutoCloseable {
  internal companion object {
    private val logger = Logger(CoordinatorComm::class)

    // A frame can at most use 1/8 of available memory. This is a magic number.
    private const val MAX_HEAP_FRACTION_PER_FRAME = 8L

    /**
     * Upper bound on the wire size of a single inbound frame.
     *
     * This is used to reject oversized frames before [Frame.decodeRaw]
     * allocates. An otherwise unrecoverable OOM can therefore be turned into a
     * catchable [FrameProcessingException].
     *
     * Deriving from [Runtime.maxMemory] means this value respects `-Xmx`. The
     * value is always clamped to [Frame.MAX_FRAME_LENGTH] from the protocol.
     */
    private val MAX_INBOUND_FRAME_SIZE: Long =
      Runtime
        .getRuntime()
        .maxMemory()
        .let {
          if (it == Long.MAX_VALUE) Frame.MAX_FRAME_LENGTH else it / MAX_HEAP_FRACTION_PER_FRAME
        }.coerceAtMost(Frame.MAX_FRAME_LENGTH)

    fun encode(outgoing: OutgoingFrame) = Frame.encodeRequest(outgoing.id, outgoing.body)

    fun decode(bytes: ByteArray) = Frame.decode(bytes)
  }

  private val nextId = AtomicInt(0)
  private val writeMutex = Mutex()
  private val stateMutex = Mutex()
  private val pending = ConcurrentHashMap<Int, CompletableDeferred<IncomingFrame>>()

  @Volatile
  private var readError: ApiError? = null
  private var dispatcherStarted = false

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
    val buffers = encode(OutgoingFrame(id, body))
    logger.debug("Sending", mapOf("id" to id, "body" to body))
    writeMutex.withLock {
      writer.writeByteArray(Frame.lengthPrefix(Frame.payloadLength(buffers)))
      for (buffer in buffers) {
        writer.writeByteArray(buffer.toByteArray())
      }
    }
  }

  @Throws(ApiError::class)
  suspend fun communicateImpl(body: Any): Any {
    val requestId = nextId.fetchAndAdd(1)
    val waiter = CompletableDeferred<IncomingFrame>()

    stateMutex.withLock {
      readError?.let { throw it }
      pending[requestId] = waiter
      if (!dispatcherStarted) {
        dispatcherStarted = true
        dispatcherScope.launch { readLoop() }
      }
    }

    // A close() concurrent with the block above either drains this waiter (it
    // was registered first) or is visible here, so no caller is left awaiting a
    // comm that is already gone.
    readError?.let {
      pending.remove(requestId)
      throw it
    }

    try {
      sendMessage(requestId, body)
    } catch (e: Throwable) {
      pending.remove(requestId)
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

  /**
   * Stop the dispatcher and fail anything still awaiting a response.
   */
  override fun close() {
    dispatcherScope.cancel()
    failAllWaiters(readError ?: ApiError("Coordinator comm closed"))
  }

  /**
   * Read one frame envelope off the wire, streaming its payload in bounded
   * chunks so an oversized frame cannot exhaust the heap in a single read.
   *
   * The declared length is rejected up front when it exceeds
   * [MAX_INBOUND_FRAME_SIZE], and an under-run payload (the prefix promised
   * more than the encoded body actually consumed) is treated as a stream
   * desync. Both surface as a [FrameProcessingException] rather than a silent
   * shutdown.
   */
  private suspend fun readRawFrame(): RawFrame {
    val prefix = reader.readByteArray(4) // First 4 bytes as length.
    if (prefix.size != 4) {
      throw ApiError("Coordinator socket closed while reading frame length")
    }

    val declaredLength = Frame.parseLengthPrefix(prefix)
    if (declaredLength.toLong() > MAX_INBOUND_FRAME_SIZE) {
      logger.error(
        "Inbound frame exceeds size limit",
        mapOf("length" to declaredLength, "limit" to MAX_INBOUND_FRAME_SIZE),
      )
      throw FrameProcessingException(
        "Inbound frame of $declaredLength bytes exceeds limit of $MAX_INBOUND_FRAME_SIZE bytes",
      )
    }

    val input = ChannelFrameInput(reader, declaredLength, currentCoroutineContext())
    val raw =
      try {
        Frame.decodeRaw(input)
      } catch (e: CancellationException) {
        throw e // Let coroutine cancellation propagate so the task coroutine unwinds.
      } catch (e: IOException) {
        logger.error(
          "Failed to read frame",
          mapOf("length" to declaredLength, "exception" to e),
        )
        throw FrameProcessingException("Failed to read frame of $declaredLength bytes", e)
      } catch (e: Exception) {
        logger.error(
          "Failed to decode frame",
          mapOf("length" to declaredLength, "exception" to e),
        )
        throw FrameProcessingException("Failed to decode frame of $declaredLength bytes", e)
      }

    if (input.remaining != 0L) {
      logger.error(
        "Frame length prefix overran the payload",
        mapOf("length" to declaredLength, "undrained" to input.remaining),
      )
      throw FrameProcessingException("Frame declared $declaredLength bytes but ${input.remaining} left unread")
    }

    return raw
  }

  private suspend fun readFrame(): IncomingFrame {
    val raw = readRawFrame()
    logger.debug("Received", mapOf("id" to raw.id))
    return IncomingFrame(raw.id, Frame.decodeBody(raw))
  }

  private suspend fun readLoop() {
    while (true) {
      val raw =
        try {
          readRawFrame()
        } catch (e: CancellationException) {
          // Coroutine cancellation is delivered by throwing this exception, and
          // cooperative cancellation requires rethrowing it so it propagates.
          throw e
        } catch (e: Throwable) {
          failAllWaiters(e)
          return
        }
      logger.debug("Received", mapOf("id" to raw.id))

      val waiter = pending.remove(raw.id)
      if (waiter == null) {
        logger.warning("Discarding response with no matching request", mapOf("id" to raw.id))
        continue
      }

      try {
        waiter.complete(IncomingFrame(raw.id, Frame.decodeBody(raw)))
      } catch (e: CancellationException) {
        throw e
      } catch (e: Throwable) {
        waiter.completeExceptionally(
          ApiError("Cannot decode response for request ${raw.id}: ${e.message}")
            .apply { initCause(e) },
        )
      }
    }
  }

  private fun failAllWaiters(cause: Throwable) {
    val error =
      cause as? ApiError
        ?: ApiError("Coordinator comm closed: ${cause.message}").apply { initCause(cause) }
    readError = error
    pending.keys.forEach { id -> pending.remove(id)?.completeExceptionally(error) }
  }
}
