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

import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.ApiError
import org.apache.airflow.sdk.execution.comm.GetVariable
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.comm.TaskInstance
import org.apache.airflow.sdk.execution.comm.XComResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import org.apache.airflow.sdk.Client as PublicClient

fun byteArrayFromHexString(hexString: String): ByteArray =
  hexString
    .split(' ', '\r', '\n')
    .filter { it.isNotEmpty() }
    .map { it.toUByte(16).toByte() }
    .toByteArray()

class CommsTest {
  @Test
  @DisplayName("Should decode startup details")
  fun shouldDecodeStartupDetails() {
    // [2, msg, null] with msg coming from
    // https://github.com/astronomer/airflow/blob/f39c8da8/task-sdk/tests/task_sdk/execution_time/test_comms.py#L73-L108
    val data =
      """
      92 02 88 a4 74 79 70 65 ae 53 74 61 72 74 75 70 44 65 74 61 69 6c 73 a2 74 69 86 a2 69 64 d9 24
      34 64 38 32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63
      65 63 61 62 a7 74 61 73 6b 5f 69 64 a1 61 aa 74 72 79 5f 6e 75 6d 62 65 72 01 a6 72 75 6e 5f 69
      64 a1 62 a6 64 61 67 5f 69 64 a1 63 ae 64 61 67 5f 76 65 72 73 69 6f 6e 5f 69 64 d9 24 34 64 38
      32 38 61 36 32 2d 61 34 31 37 2d 34 39 33 36 2d 61 37 61 36 2d 32 62 33 66 61 62 61 63 65 63 61
      62 aa 74 69 5f 63 6f 6e 74 65 78 74 85 a7 64 61 67 5f 72 75 6e 8c a6 64 61 67 5f 69 64 a1 63 a6
      72 75 6e 5f 69 64 a1 62 ac 6c 6f 67 69 63 61 6c 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31
      54 30 31 3a 30 30 3a 30 30 5a b3 64 61 74 61 5f 69 6e 74 65 72 76 61 6c 5f 73 74 61 72 74 b4 32
      30 32 34 2d 31 32 2d 30 31 54 30 30 3a 30 30 3a 30 30 5a b1 64 61 74 61 5f 69 6e 74 65 72 76 61
      6c 5f 65 6e 64 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a aa 73 74 61 72 74
      5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a9 72 75 6e 5f 61
      66 74 65 72 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a a8 65 6e 64 5f 64 61
      74 65 c0 a8 72 75 6e 5f 74 79 70 65 a6 6d 61 6e 75 61 6c a5 73 74 61 74 65 a7 73 75 63 63 65 73
      73 a4 63 6f 6e 66 c0 b5 63 6f 6e 73 75 6d 65 64 5f 61 73 73 65 74 5f 65 76 65 6e 74 73 90 a9 6d
      61 78 5f 74 72 69 65 73 00 ac 73 68 6f 75 6c 64 5f 72 65 74 72 79 c2 a9 76 61 72 69 61 62 6c 65
      73 c0 ab 63 6f 6e 6e 65 63 74 69 6f 6e 73 c0 a4 66 69 6c 65 a9 2f 64 65 76 2f 6e 75 6c 6c aa 73
      74 61 72 74 5f 64 61 74 65 b4 32 30 32 34 2d 31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a ac 64
      61 67 5f 72 65 6c 5f 70 61 74 68 a9 2f 64 65 76 2f 6e 75 6c 6c ab 62 75 6e 64 6c 65 5f 69 6e 66
      6f 82 a4 6e 61 6d 65 a8 61 6e 79 2d 6e 61 6d 65 a7 76 65 72 73 69 6f 6e ab 61 6e 79 2d 76 65 72
      73 69 6f 6e b2 73 65 6e 74 72 79 5f 69 6e 74 65 67 72 61 74 69 6f 6e a0 c0
      """.trimIndent()
    val result = CoordinatorComm.decode(byteArrayFromHexString(data))
    Assertions.assertInstanceOf(IncomingFrame::class.java, result)
    Assertions.assertInstanceOf(StartupDetails::class.java, result.body)
  }

  @Test
  @DisplayName("Should serialize all fields")
  fun shouldEncodeSucceedTask() {
    val endDate = OffsetDateTime.of(2024, 12, 1, 1, 0, 0, 0, ZoneOffset.UTC)
    val bytes = CoordinatorComm.encode(OutgoingFrame(3, TaskResult.success(endDate = endDate)))
    val actual = bytes.toHexString(HexFormat { bytes { byteSeparator = " " } })

    val expected =
      """
      92 03 85 a5 73 74 61 74 65 a7 73 75 63 63 65 73 73 a8 65 6e 64 5f 64 61 74 65 b4 32 30 32 34 2d
      31 32 2d 30 31 54 30 31 3a 30 30 3a 30 30 5a ac 74 61 73 6b 5f 6f 75 74 6c 65 74 73 90 ad 6f 75
      74 6c 65 74 5f 65 76 65 6e 74 73 90 a4 74 79 70 65 ab 53 75 63 63 65 65 64 54 61 73 6b
      """.trimIndent().replace('\n', ' ')

    Assertions.assertEquals(expected, actual)
  }

  private fun responseFrame(
    id: Int,
    key: String = "return_value",
  ): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(3)
      packer.packInt(id)
      packer.packMapHeader(3)
      packer.packString("type")
      packer.packString("XComResult")
      packer.packString("key")
      packer.packString(key)
      packer.packString("value")
      packer.packInt(1)
      packer.packNil()
    }
    return out.toByteArray()
  }

  private suspend fun ByteChannel.writeFrame(payload: ByteArray) {
    writeByteArray(Frame.lengthPrefix(payload.size))
    writeByteArray(payload)
  }

  @Test
  @DisplayName("Should discard a frame with no matching waiter and still deliver the correct response")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun discardsUnmatchedFrameAndDeliversCorrectResponse() {
    val toClient = ByteChannel(autoFlush = true)
    val fromClient = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toClient, fromClient)

    val result =
      runBlocking {
        // A frame whose id (99) matches no in-flight request.
        // The dispatcher must drop the stray frame and still hand id 0 to its waiter.
        toClient.writeFrame(responseFrame(99, key = "stray"))
        toClient.writeFrame(responseFrame(0, key = "return_value"))
        comm.communicate<XComResult>(GetVariable().also { it.key = "k" })
      }

    Assertions.assertEquals("return_value", result.key)
    comm.close()
  }

  @Test
  @DisplayName("Should keep many requests in flight and match out-of-order responses")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun allowsMultipleInFlightRequestsAnsweredOutOfOrder() {
    val toClient = ByteChannel(autoFlush = true)
    val fromClient = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toClient, fromClient)
    val n = 10

    // Collect every request before answering any, then reply in reverse order.
    val server =
      Thread {
        runBlocking {
          val ids =
            (0 until n).map {
              val prefix = fromClient.readByteArray(4)
              val payload = fromClient.readByteArray(Frame.parseLengthPrefix(prefix))
              CoordinatorComm.decode(payload).id
            }
          ids.reversed().forEach { toClient.writeFrame(responseFrame(it)) }
        }
      }
    server.start()

    val errors = ConcurrentLinkedQueue<Throwable>()
    val results = ConcurrentLinkedQueue<XComResult>()
    val workers =
      (1..n).map {
        Thread {
          try {
            results.add(runBlocking { comm.communicate<XComResult>(GetVariable().also { it.key = "k" }) })
          } catch (e: Throwable) {
            errors.add(e)
          }
        }
      }
    workers.forEach { it.start() }
    workers.forEach { it.join() }
    server.join()

    Assertions.assertTrue(errors.isEmpty(), "concurrent in-flight calls failed: $errors")
    Assertions.assertEquals(n, results.size)
    comm.close()
  }

  @Test
  @DisplayName("Should stay correlated when the client is called from many threads")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun publicClientSurvivesConcurrentThreadCalls() {
    val toClient = ByteChannel(autoFlush = true)
    val fromClient = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toClient, fromClient)
    val details =
      StartupDetails().also {
        it.ti =
          TaskInstance().also { ti ->
            ti.dagId = "d"
            ti.runId = "r"
          }
      }
    val client = PublicClient(details, CoordinatorClient(comm))
    val n = 50

    val server =
      Thread {
        runBlocking {
          repeat(n) {
            val prefix = fromClient.readByteArray(4)
            val payload = fromClient.readByteArray(Frame.parseLengthPrefix(prefix))
            toClient.writeFrame(responseFrame(CoordinatorComm.decode(payload).id))
          }
        }
      }
    server.start()

    val errors = ConcurrentLinkedQueue<Throwable>()
    val results = ConcurrentLinkedQueue<Any?>()
    val workers =
      (1..n).map {
        Thread {
          try {
            results.add(client.getXCom(taskId = "upstream"))
          } catch (e: Throwable) {
            errors.add(e)
          }
        }
      }
    workers.forEach { it.start() }
    workers.forEach { it.join() }
    server.join()

    Assertions.assertTrue(errors.isEmpty(), "concurrent public-client calls failed: $errors")
    Assertions.assertEquals(n, results.size)
    comm.close()
  }

  @Test
  @DisplayName("Should fail a pending call when the coordinator socket closes")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun failsPendingCallWhenSocketCloses() {
    val toClient = ByteChannel(autoFlush = true)
    val fromClient = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toClient, fromClient)

    Assertions.assertThrows(ApiError::class.java) {
      runBlocking {
        val call = async { comm.communicate<XComResult>(GetVariable().also { it.key = "k" }) }
        // No response is ever written; closing the read side must surface an
        // error to the waiting caller instead of hanging forever.
        toClient.flushAndClose()
        call.await()
      }
    }
    comm.close()
  }
}
