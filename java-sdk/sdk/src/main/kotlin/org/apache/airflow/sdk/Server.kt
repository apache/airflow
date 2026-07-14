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

import com.xenomachina.argparser.ArgParser
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.LogSender
import org.apache.airflow.sdk.execution.Logger
import kotlin.text.substringAfterLast
import kotlin.text.substringBeforeLast

private class Args(
  parser: ArgParser,
) {
  private fun parseAddress(address: String): InetSocketAddress =
    InetSocketAddress(
      address.substringBeforeLast(':'),
      address.substringAfterLast(':').toInt(),
    )

  val comm by parser.storing("--comm", help = "Address (host:port) to communicate with parent") {
    parseAddress(this)
  }
  val logs by parser.storing("--logs", help = "Address (host:port) to send Airflow logs to") {
    parseAddress(this)
  }
}

/**
 * Thrown when an Airflow API call returns an error response.
 *
 * Extends [IllegalStateException] so callers can handle it without
 * checked-exception machinery.
 */
class ApiError(
  message: String,
) : IllegalStateException(message)

/**
 * Connects this JVM process to the Airflow coordinator and dispatches task
 * execution requests to the registered [Bundle].
 *
 * The typical entry point is:
 *
 * ```java
 * public static void main(String[] args) {
 *     Server.create(args).serve(new MyBundleBuilder().build());
 * }
 * ```
 *
 * The process exits when the coordinator closes the connection (normally after
 * one task-instance execution).
 */
class Server(
  private val comm: InetSocketAddress,
  private val logs: InetSocketAddress,
) {
  companion object {
    /**
     * Parses coordinator addresses from command-line arguments and returns a
     * ready-to-use [Server].
     *
     * The arguments are supplied automatically by Airflow and are not intended
     * to be constructed by hand:
     *
     * * `--comm host:port` address for task-execution messages.
     * * `--logs host:port` address for log forwarding.
     *
     * @param args Command-line arguments as received by `main`.
     * @return A configured [Server] ready to call [serve].
     */
    @JvmStatic
    fun create(args: Array<String>): Server {
      val args = ArgParser(args).parseInto(::Args)
      return Server(args.comm, args.logs)
    }
  }

  private val logger = Logger(Server::class)

  /**
   * Blocking entry point: connects to the coordinator and serves task-execution
   * requests from the given [bundle].
   *
   * This is a convenience wrapper around [serveAsync] for use from a plain
   * `main` method. Prefer [serveAsync] when calling from an existing coroutine.
   * The call returns when the coordinator closes the connection (normally after
   * one task-instance execution).
   *
   * @param bundle Bundle containing all Dags this process can execute.
   *
   * @see [serveAsync]
   */
  fun serve(bundle: Bundle) {
    runBlocking { launch { serveAsync(bundle) } }
  }

  /**
   * Suspending entry point: connects to the coordinator and serves
   * task-execution requests from the given [bundle].
   *
   * Opens both the task-execution channel (`--comm`) and the log-forwarding
   * channel (`--logs`) concurrently, then processes incoming requests until the
   * coordinator closes the connection (normally after one task-instance
   * execution). The coroutine returns once both channels have been closed.
   *
   * Use this variant when calling from an existing coroutine scope; use the
   * blocking [serve] from a plain `main` method.
   *
   * @param bundle Bundle containing all Dags this process can execute.
   *
   * @see [serve]
   */
  suspend fun serveAsync(bundle: Bundle) =
    coroutineScope {
      val deferral = CompletableDeferred<Unit>()

      launch {
        try {
          aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(comm).use { socket ->
            logger.debug("Connected comm", mapOf("addr" to comm))
            CoordinatorComm(
              bundle,
              socket.openReadChannel(),
              socket.openWriteChannel(autoFlush = true),
            ).startProcessing()
          }
        } finally {
          deferral.complete(Unit)
        }
      }
      launch {
        aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(logs).use { socket ->
          logger.debug("Connected logs", mapOf("addr" to logs))
          LogSender.configure(socket.openWriteChannel(autoFlush = true))
          deferral.await()
        }
      }
    }
}
