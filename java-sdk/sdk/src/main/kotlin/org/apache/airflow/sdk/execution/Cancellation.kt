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

import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.StoppableTask
import sun.misc.Signal
import sun.misc.SignalHandler
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Records whether Airflow has asked the running task to stop. For a [StoppableTask], also catches the
 * signal that carries that request.
 */
internal class Cancellation : AutoCloseable {
  private val claimed = AtomicBoolean()
  private val requested = CountDownLatch(1)
  private val signalHandled = CompletableFuture<Unit>()

  @Volatile
  private var held = false

  @Volatile
  private var trap: AutoCloseable? = null

  @Volatile
  private var hook: (() -> Unit)? = null

  fun check(): Boolean {
    throwIfTaskCannotSeeRequest()
    return requested.count == 0L
  }

  fun await(timeout: Duration): Boolean {
    throwIfTaskCannotSeeRequest()
    return requested.await(TimeUnit.NANOSECONDS.convert(timeout), TimeUnit.NANOSECONDS)
  }

  private fun throwIfTaskCannotSeeRequest() {
    check(held) {
      "Only a StoppableTask can see a stop request. Implement StoppableTask, or give an annotated task an @Airflow.OnKill method"
    }
  }

  /**
   * Catches the first stop signal for [task] until [close] is called. The SDK calls this method just before it starts the task.
   *
   * @throws CannotCatchStopSignalsException if the SDK can catch neither stop signal. If the task ran anyway, it would never
   *   see a stop request, and it could start work, such as a job in an external system, that its onKill could not stop.
   */
  fun hold(
    task: StoppableTask,
    context: Context,
  ) {
    val classLoader = Thread.currentThread().contextClassLoader
    hook = {
      // On Java 11 and 17, a signal handler thread has no context class loader, and libraries can need one.
      Thread.currentThread().contextClassLoader = classLoader
      task.onKill(context)
    }
    // A signal can reach onKill before trapStopSignals returns. If onKill then checks for a stop request,
    // that check reads held, so set held before calling trapStopSignals.
    held = true
    trap = trapStopSignals(::takeSignal)
      ?: throw CannotCatchStopSignalsException(
        "The SDK cannot catch SIGTERM or SIGINT in this JVM, so the task cannot see a stop request. The JVM might run with -Xrs, " +
          "or without the jdk.unsupported module",
      )
  }

  /**
   * Records that the task has returned, so a later signal does not call [StoppableTask.onKill]. If a
   * signal has already started that call, waits for it to finish, because the JVM exits soon after the
   * SDK reports the task's result. The wait has no limit of its own, since the supervisor's SIGKILL ends
   * a call that hangs.
   */
  fun finish() {
    hook = null
    // join() keeps waiting if the thread is interrupted, and a task can leave the thread's interrupt flag set.
    if (claimed.get()) signalHandled.join()
  }

  override fun close() {
    trap?.close()
  }

  private fun takeSignal(signal: String): Boolean {
    // A second signal means the sender does not want to wait.
    if (!claimed.compareAndSet(false, true)) {
      logger.info("Letting the JVM handle the signal", mapOf("signal" to signal))
      return false
    }
    return try {
      logger.warning("Asking the task to stop", mapOf("signal" to signal))
      // Record the stop request before the hook reads the task's fields. If a task stores what the hook
      // needs and then checks for the request, the task sees the request, the hook sees the stored field,
      // or both.
      requested.countDown()
      hook?.invoke()
      true
    } catch (e: Throwable) {
      logger.error(
        "Task's onKill failed, so letting the JVM handle the signal",
        mapOf("signal" to signal, "error" to e, "trace" to e.stackTraceToString()),
      )
      false
    } finally {
      signalHandled.complete(Unit)
    }
  }
}

/** Thrown by [Cancellation.hold] when the SDK can catch neither stop signal in this JVM. */
internal class CannotCatchStopSignalsException(
  message: String,
) : IllegalStateException(message)

// The thread that handles a stop signal uses this logger. If that thread waited for room in LogSender while nothing
// reads the logs, the task would never see the stop request. A second signal could not reach the JVM either.
private val logger = Logger(Cancellation::class, waitsForRoom = false)

/**
 * Opens a trap that passes SIGINT and SIGTERM to [onSignal] instead of the JVM. The trap stays open
 * until the caller closes the returned handle. If the SDK can catch neither signal, for example because
 * the JVM runs with -Xrs, no trap opens and this function returns null.
 *
 * When [onSignal] returns false or throws, the JVM gets its own handler back and receives the
 * signal again, so the JVM handles the signal as it would have without the trap. Only one trap can
 * be open at a time, because closing a trap puts back the handlers that were installed when that
 * trap opened.
 */
internal fun trapStopSignals(onSignal: (signal: String) -> Boolean): AutoCloseable? {
  val restores = listOf("INT", "TERM").mapNotNull { trapSignal(it, onSignal) }
  if (restores.isEmpty()) return null
  return AutoCloseable { restores.forEach { restore -> restore() } }
}

private fun trapSignal(
  name: String,
  onSignal: (signal: String) -> Boolean,
): (() -> Unit)? =
  try {
    val signal = Signal(name)
    val previous = AtomicReference<SignalHandler>()
    previous.set(
      Signal.handle(signal) {
        if (!runCatching { onSignal("SIG$name") }.getOrDefault(false)) {
          // A signal can arrive before Signal.handle has returned the handler it replaced.
          while (previous.get() == null) Thread.onSpinWait()
          Signal.handle(signal, previous.get())
          Signal.raise(signal)
        }
      },
    )
    ({ Signal.handle(signal, previous.get()) })
  } catch (e: IllegalArgumentException) {
    // The JVM refuses the signal, for example when it was started with -Xrs.
    logger.warning("Cannot catch signal, so the task cannot see it", mapOf("signal" to "SIG$name", "error" to e))
    null
  } catch (e: LinkageError) {
    // sun.misc.Signal lives in the jdk.unsupported module, which a jlink runtime image can leave out.
    logger.warning("Cannot catch signal, so the task cannot see it", mapOf("signal" to "SIG$name", "error" to e))
    null
  }
