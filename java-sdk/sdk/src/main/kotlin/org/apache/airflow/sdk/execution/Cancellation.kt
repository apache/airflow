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
    if (!held) {
      throw TaskCannotSeeStopRequestException(
        "Only a StoppableTask can see a stop request. Implement StoppableTask, or give an annotated task an @Airflow.OnKill method",
      )
    }
  }

  /** Catches the first stop signal for [task] until [close], or throws [CannotCatchStopSignalsException]. */
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
    // Set first: a signal can run onKill, which may check for a stop request, before trapStopSignals returns.
    held = true
    trap = trapStopSignals(::takeSignal)
      ?: throw CannotCatchStopSignalsException(
        "The SDK cannot catch SIGTERM or SIGINT in this JVM, so the task cannot see a stop request. The JVM might run with -Xrs, " +
          "or without the jdk.unsupported module",
      )
  }

  /**
   * Stops later signals from calling onKill and waits for a call that already started.
   * The wait has no timeout because the supervisor's SIGKILL ends a call that hangs.
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
      val hook = hook
      if (hook == null) {
        // finish() clears hook once execute returns or throws. The signal trap stays open until the SDK sends the result.
        logger.warning("Holding the signal until the SDK has sent the task's result", mapOf("signal" to signal))
      } else {
        logger.warning("Asking the task to stop", mapOf("signal" to signal))
      }
      // Record first: a task that stores state and then checks sees the request, onKill sees the state, or both.
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

/** Thrown by [Context.isCancelled] and [Context.awaitCancellation] when the running task is not a [StoppableTask]. */
internal class TaskCannotSeeStopRequestException(
  message: String,
) : IllegalStateException(message)

// The stop-signal thread never waits for room: unread logs could block the stop request and a second signal.
private val logger = Logger(Cancellation::class, waitsForRoom = false)

/**
 * Passes SIGINT and SIGTERM to [onSignal] until the handle is closed, or returns null if neither can be caught.
 * If [onSignal] returns false or throws, the JVM handles the signal. Only one trap can be open at a time.
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
