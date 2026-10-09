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

import io.ktor.network.sockets.InetSocketAddress
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.Frame
import org.apache.airflow.sdk.execution.IncomingFrame
import org.apache.airflow.sdk.execution.RawFrame
import org.apache.airflow.sdk.execution.comm.TaskState
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.msgpack.core.MessagePack
import org.msgpack.core.buffer.ArrayBufferInput
import java.io.ByteArrayOutputStream
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

private class ServerNoopTask : Task {
  override fun execute(
    context: Context,
    client: Client,
  ) = Unit
}

class ServerTest {
  private fun hexToBytes(hex: String): ByteArray =
    hex
      .split(' ', '\r', '\n')
      .filter { it.isNotEmpty() }
      .map { it.toUByte(16).toByte() }
      .toByteArray()

  private fun ackFrame(id: Int): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(2)
      packer.packInt(id)
      packer.packNil()
    }
    return out.toByteArray()
  }

  private suspend fun ByteChannel.writeFrame(payload: ByteArray) {
    writeByteArray(Frame.lengthPrefix(payload.size.toUInt()))
    writeByteArray(payload)
  }

  private fun errorResponseFrame(id: Int): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(3)
      packer.packInt(id)
      packer.packNil()
      packer.packMapHeader(3)
      packer.packString("type")
      packer.packString("ErrorResponse")
      packer.packString("error")
      packer.packString("API_SERVER_ERROR")
      packer.packString("detail")
      packer.packMapHeader(1)
      packer.packString("status_code")
      packer.packInt(500)
    }
    return out.toByteArray()
  }

  private fun startupFrame(
    mapIndex: Int?,
    tiContext: Map<String, Any?>,
  ): ByteArray {
    val body =
      mapOf(
        "type" to "StartupDetails",
        "ti" to
          mapOf(
            "id" to "4d828a62-a417-4936-a7a6-2b3fabacecab",
            "task_id" to "extract",
            "dag_id" to "dag1",
            "run_id" to "run1",
            "try_number" to 2,
            "map_index" to mapIndex,
            "dag_version_id" to "4d828a62-a417-4936-a7a6-2b3fabacecab",
          ),
        "ti_context" to mapOf("dag_run" to mapOf("dag_id" to "dag1", "run_id" to "run1"), "max_tries" to 1) + tiContext,
        "dag_rel_path" to "/dev/null",
        "bundle_info" to mapOf("name" to "any-name", "version" to "any-version"),
        "start_date" to "2024-12-01T01:00:00Z",
        "sentry_integration" to "",
      )
    return Frame.encodeRequest(0, body).fold(ByteArray(0)) { acc, buffer -> acc + buffer.toByteArray() }
  }

  /**
   * Runs [taskClass] as task "extract" of Dag "dag1" through [CoordinatorServer.dispatchTask]. The
   * fake supervisor first sends StartupDetails, with [mapIndex] as the map_index of the task
   * instance and [tiContext] added to ti_context. Until the task reports its final state, [answer]
   * builds the reply to each request from the request ID. Returns the bodies of those requests in
   * arrival order, and the body of the final state.
   */
  private fun serveTask(
    taskClass: Class<out Task>,
    mapIndex: Int?,
    tiContext: Map<String, Any?>,
    answer: (Int) -> ByteArray = ::ackFrame,
  ): Pair<List<Map<*, *>>, Map<*, *>> {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))
    val bundle = Bundle(listOf(DagDef("dag1").addTask(TaskDef("extract", taskClass))))

    val requests = mutableListOf<Map<*, *>>()
    var terminal: Map<*, *>? = null
    val supervisor =
      Thread {
        runBlocking {
          toServer.writeFrame(startupFrame(mapIndex, tiContext))
          while (terminal == null) {
            val prefix = fromServer.readByteArray(4)
            val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
            val request = Frame.decodeRaw(ArrayBufferInput(payload))
            val body = request.rawBody as Map<*, *>
            if (body["type"] in setOf("SucceedTask", "TaskState", "RetryTask")) {
              terminal = body
              toServer.writeFrame(ackFrame(request.id))
            } else {
              requests += body
              toServer.writeFrame(answer(request.id))
            }
          }
        }
      }
    supervisor.start()

    runBlocking { server.dispatchTask(bundle, comm) }
    supervisor.join()
    comm.close()
    return requests to checkNotNull(terminal)
  }

  private fun deleteXComRequest(
    key: String,
    mapIndex: Int,
  ) = mapOf(
    "type" to "DeleteXCom",
    "key" to key,
    "dag_id" to "dag1",
    "run_id" to "run1",
    "task_id" to "extract",
    "map_index" to mapIndex.toLong(),
  )

  @Test
  @DisplayName("Should run the task and report the result as a normal awaited request")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun runsTaskAndReportsResultAsAwaitedRequest() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))

    val reported = ArrayBlockingQueue<IncomingFrame>(1)
    val supervisor =
      Thread {
        runBlocking {
          // Deliver the StartupDetails frame (id 2, dag_id "c", task_id "a").
          toServer.writeFrame(hexToBytes(STARTUP_HEX))
          val prefix = fromServer.readByteArray(4)
          val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
          val result = CoordinatorComm.decode(payload)
          reported.put(result)
          toServer.writeFrame(ackFrame(result.id))
        }
      }
    supervisor.start()

    runBlocking { server.dispatchTask(Bundle(emptyList()), comm) }
    supervisor.join()

    val result = reported.take()
    Assertions.assertEquals(0, result.id)
    Assertions.assertInstanceOf(TaskState::class.java, result.body)
    Assertions.assertEquals(TaskState.State.REMOVED, (result.body as TaskState).state)
    comm.close()
  }

  @Test
  @DisplayName("Should fail when the initial frame is not startup details")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun failsWhenInitialFrameIsNotStartupDetails() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))

    Assertions.assertThrows(ApiError::class.java) {
      runBlocking {
        toServer.writeFrame(ackFrame(7))
        server.dispatchTask(Bundle(emptyList()), comm)
      }
    }
    comm.close()
  }

  @Test
  @DisplayName("Should serialize the bundle's dags when the initial frame is a parse request")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun parsesDagsAndReportsResult() {
    val toServer = ByteChannel(autoFlush = true)
    val fromServer = ByteChannel(autoFlush = true)
    val comm = CoordinatorComm(toServer, fromServer)
    val server = CoordinatorServer(InetSocketAddress("localhost", 0), InetSocketAddress("localhost", 0))
    val bundle = Bundle(listOf(DagDef("parsed_dag").addTask(TaskDef("t", ServerNoopTask::class.java))))

    val reported = ArrayBlockingQueue<RawFrame>(1)
    val supervisor =
      Thread {
        runBlocking {
          toServer.writeFrame(parseRequestFrame(3, "/bundle/dags/java.py", "/bundle"))
          val prefix = fromServer.readByteArray(4)
          val payload = fromServer.readByteArray(Frame.parseLengthPrefix(prefix).toInt())
          val raw = Frame.decodeRaw(ArrayBufferInput(payload))
          reported.put(raw)
          toServer.writeFrame(ackFrame(raw.id))
        }
      }
    supervisor.start()

    runBlocking { server.dispatchTask(bundle, comm) }
    supervisor.join()

    val body = reported.take().rawBody as Map<*, *>
    Assertions.assertEquals("DagFileParsingResult", body["type"])
    Assertions.assertEquals("/bundle/dags/java.py", body["fileloc"])
    val dags = body["serialized_dags"] as List<*>
    Assertions.assertEquals(1, dags.size)
    val dag = ((dags[0] as Map<*, *>)["data"] as Map<*, *>)["dag"] as Map<*, *>
    Assertions.assertEquals("parsed_dag", dag["dag_id"])
    comm.close()
  }

  private fun parseRequestFrame(
    id: Int,
    file: String,
    bundlePath: String,
  ): ByteArray {
    val out = ByteArrayOutputStream()
    MessagePack.newDefaultPacker(out).use { packer ->
      packer.packArrayHeader(2)
      packer.packInt(id)
      packer.packMapHeader(3)
      packer.packString("type").packString("DagFileParseRequest")
      packer.packString("file").packString(file)
      packer.packString("bundle_path").packString(bundlePath)
    }
    return out.toByteArray()
  }

  @Test
  @DisplayName("Should delete each listed XCom before the task sends anything")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun deletesListedXComsBeforeTheTaskSendsAnything() {
    val cases =
      listOf(
        Triple(-1, listOf("return_value", "summary"), -1),
        Triple(0, listOf("return_value", "summary"), 0),
        Triple(2, listOf("return_value", "summary"), 2),
        Triple(null, listOf("return_value"), -1),
        Triple(-1, emptyList(), -1),
        Triple(-1, null, -1),
      )
    for ((mapIndex, keys, deletedMapIndex) in cases) {
      val (requests, terminal) = serveTask(PushingTask::class.java, mapIndex, mapOf("xcom_keys_to_clear" to keys))

      Assertions.assertEquals(keys.orEmpty().map { deleteXComRequest(it, deletedMapIndex) }, requests.dropLast(1)) {
        "unexpected requests for map_index $mapIndex and keys $keys: $requests"
      }
      Assertions.assertEquals("SetXCom", requests.last()["type"])
      Assertions.assertEquals("SucceedTask", terminal["type"])
    }
  }

  @Test
  @DisplayName("Should fail without running the task when an XCom cannot be deleted")
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  fun failsWithoutRunningTheTaskWhenAnXComCannotBeDeleted() {
    for (shouldRetry in listOf(true, false)) {
      val (requests, terminal) =
        serveTask(
          PushingTask::class.java,
          -1,
          mapOf("xcom_keys_to_clear" to listOf("return_value", "summary"), "should_retry" to shouldRetry),
          ::errorResponseFrame,
        )

      Assertions.assertEquals(listOf(deleteXComRequest("return_value", -1)), requests) {
        "the runtime should stop at the first XCom it cannot delete and not run the task"
      }
      if (shouldRetry) {
        Assertions.assertEquals("RetryTask", terminal["type"])
      } else {
        Assertions.assertEquals("TaskState", terminal["type"])
        Assertions.assertEquals("failed", terminal["state"])
      }
    }
  }

  class PushingTask : Task {
    override fun execute(
      context: Context,
      client: Client,
    ) {
      client.setXCom(value = "rows")
    }
  }

  private companion object {
    // [2, msg, null] with msg coming from
    // https://github.com/astronomer/airflow/blob/f39c8da8/task-sdk/tests/task_sdk/execution_time/test_comms.py#L73-L108
    val STARTUP_HEX =
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
  }
}
