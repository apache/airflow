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

import com.fasterxml.jackson.databind.ObjectMapper
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.MissingValueException
import com.xenomachina.argparser.default
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.airflow.sdk.execution.Cancellation
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.LogSender
import org.apache.airflow.sdk.execution.Logger
import org.apache.airflow.sdk.execution.comm.DagFileParseRequest
import org.apache.airflow.sdk.execution.comm.ErrorResponse
import org.apache.airflow.sdk.execution.comm.StartupDetails
import org.apache.airflow.sdk.execution.parseDags
import org.apache.airflow.sdk.execution.runTask
import java.io.File
import java.time.Duration
import kotlin.text.substringAfterLast
import kotlin.text.substringBeforeLast

// The shutdown hook stops waiting for the queued log lines once none has gone out for this long. When the lines did
// go out, the hook also waits at most this long for the socket to close. The supervisor can stop reading the logs
// for several seconds, for example while it waits for a slow request to the API server. The hook should not lose the
// last lines during such a pause. Tests set a shorter timeout.
internal var logFlushTimeout: Duration = Duration.ofSeconds(30)

/**
 * Writes the queued log lines, and waits until the socket has sent them and closed. Closing the socket starts
 * sending what is left in its buffer, so without the wait, the JVM can exit before the last lines reach the
 * supervisor. The JVM cannot exit until this shutdown hook returns. So the hook stops waiting for the queued lines
 * once none has gone out for [logFlushTimeout]. When the lines did go out, the hook then waits at most that long for
 * the socket to close.
 *
 * @param close Closes the socket.
 * @param closed Completes when the socket has closed.
 */
internal fun sendQueuedLogs(
  close: () -> Unit,
  closed: Job,
) {
  val written = LogSender.detachAndFlush(logFlushTimeout)
  close()
  // If detachAndFlush gave up, nothing read the logs, or something read them far too slowly, and detachAndFlush has
  // cancelled the channel of the log socket. Waiting for the socket to close would then only delay the JVM's exit.
  // While nothing reads the socket, ktor keeps trying to write the socket's buffer without a pause, so cancelling the
  // channel does not end a wait for the socket to close.
  if (written) runBlocking { withTimeoutOrNull(logFlushTimeout.toMillis()) { closed.join() } }
}

private class Args(
  parser: ArgParser,
) {
  private fun parseAddress(address: String): InetSocketAddress =
    InetSocketAddress(
      address.substringBeforeLast(':'),
      address.substringAfterLast(':').toInt(),
    )

  val comm by parser
    .storing("--comm", help = "Address (host:port) to communicate with parent") { parseAddress(this) }
    .default(null)
  val logs by parser
    .storing("--logs", help = "Address (host:port) to send Airflow logs to") { parseAddress(this) }
    .default(null)
  val describeSources by parser
    .storing("--describe-sources", help = "File to write each Dag's declaring class to, instead of serving") {
      File(this)
    }.default(null)
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
sealed class Server {
  /**
   * Blocking entry point: connects to the coordinator and serves task-execution
   * requests from the given [bundle].
   *
   * This is a convenience wrapper around [serveAsync] for use from a plain
   * `main` method. Prefer [serveAsync] when calling from an existing coroutine.
   * The call returns when the coordinator closes the connection (normally after
   * one task-instance execution).
   *
   * A [Server] created from `--describe-sources` writes that file and returns
   * without connecting anywhere.
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
   * A [Server] created from `--describe-sources` writes that file and returns
   * without connecting anywhere.
   *
   * Use this variant when calling from an existing coroutine scope; use the
   * blocking [serve] from a plain `main` method.
   *
   * @param bundle Bundle containing all Dags this process can execute.
   *
   * @see [serve]
   */
  abstract suspend fun serveAsync(bundle: Bundle)

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
     * * `--describe-sources file` makes [serve] write a JSON object mapping each
     *   Java-declared Dag ID to the binary name of its declaring class into `file`
     *   and return without connecting anywhere; `--comm` and `--logs` are not
     *   required then. It is for the Gradle plugin, not for users.
     *
     * @param args Command-line arguments as received by `main`.
     * @return A configured [Server] ready to call [serve].
     */
    @JvmStatic
    fun create(args: Array<String>): Server {
      val args = ArgParser(args).parseInto(::Args)
      args.describeSources?.let { return SourceDescriber(it) }
      return CoordinatorServer(
        args.comm ?: throw MissingValueException("--comm"),
        args.logs ?: throw MissingValueException("--logs"),
      )
    }
  }
}

/** Writes each Java-declared Dag's declaring class to [target] instead of serving. */
internal class SourceDescriber(
  private val target: File,
) : Server() {
  override suspend fun serveAsync(bundle: Bundle) {
    bundle.finalizeRegistration()
    val sources = linkedMapOf<String, String>()
    bundle.dags.values.forEach { dag -> dag.declaringClass?.let { sources[dag.id] = it.name } }
    target.absoluteFile.parentFile?.mkdirs()
    ObjectMapper().writeValue(target, sources)
  }
}

/** Serves task-execution requests over the coordinator's [comm] and [logs] sockets. */
class CoordinatorServer(
  private val comm: InetSocketAddress,
  private val logs: InetSocketAddress,
) : Server() {
  private val logger = Logger(CoordinatorServer::class)

  override suspend fun serveAsync(bundle: Bundle) {
    coroutineScope {
      bundle.finalizeRegistration()
      val deferral = CompletableDeferred<Unit>()

      launch {
        try {
          SelectorManager(Dispatchers.IO).use { selector ->
            aSocket(selector).tcp().connect(comm).use { socket ->
              logger.debug("Connected comm", mapOf("addr" to comm))
              CoordinatorComm(
                socket.openReadChannel(),
                socket.openWriteChannel(autoFlush = true),
              ).use { coordinator ->
                dispatchTask(bundle, coordinator)
              }
            }
          }
        } finally {
          deferral.complete(Unit)
        }
      }
      launch {
        SelectorManager(Dispatchers.IO).use { selector ->
          aSocket(selector).tcp().connect(logs).use { socket ->
            logger.debug("Connected logs", mapOf("addr" to logs))
            LogSender.configure(socket.openWriteChannel(autoFlush = true))
            Runtime.getRuntime().addShutdownHook(Thread({ sendQueuedLogs(socket::close, socket.socketContext) }, "Airflow log flush"))
            try {
              deferral.await()
            } finally {
              // The socket closes when the enclosing use block ends, even when serving fails, so write the
              // queued lines while the socket is still open.
              LogSender.detachAndFlush()
            }
          }
        }
      }
    }
  }

  internal suspend fun dispatchTask(
    bundle: Bundle,
    coordinator: CoordinatorComm,
  ) {
    val frame = coordinator.readMessage()
    when (val body = frame.body) {
      is StartupDetails -> runTaskAndReport(bundle, body, coordinator)
      is DagFileParseRequest -> parseDagsAndReport(bundle, body, coordinator)
      is ErrorResponse -> throw ApiError("[${body.error}] ${body.detail}")
      else -> throw ApiError("Unexpected initial frame (id=${frame.id})")
    }
  }

  private suspend fun runTaskAndReport(
    bundle: Bundle,
    startup: StartupDetails,
    coordinator: CoordinatorComm,
  ) {
    // For a StoppableTask, the signal trap stays open until the SDK reports the task's result. If the
    // first signal arrives during the report, it therefore cannot stop the JVM before the result is sent.
    Cancellation().use { cancellation ->
      val result = runTask(bundle, startup, coordinator, cancellation)
      cancellation.finish()
      // The task or its onKill can leave this thread interrupted. runBlocking in serve would then throw
      // InterruptedException, and the JVM would exit with code 1 before the supervisor acknowledges the
      // task's result.
      if (Thread.interrupted()) {
        logger.info("Cleared the interrupt flag that the task left on its thread, so the SDK can report the result")
      }
      coordinator.communicate<Unit>(result)
    }
  }

  private suspend fun parseDagsAndReport(
    bundle: Bundle,
    request: DagFileParseRequest,
    coordinator: CoordinatorComm,
  ) {
    coordinator.communicate<Unit>(parseDags(bundle, request))
  }
}
