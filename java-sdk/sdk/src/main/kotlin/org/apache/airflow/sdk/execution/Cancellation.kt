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

import sun.misc.Signal
import sun.misc.SignalHandler
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Records whether Airflow has asked the running task to stop, and whether the task checks for that
 * request.
 */
internal class Cancellation {
  private val claimed = AtomicBoolean()
  private val requested = CountDownLatch(1)

  /**
   * True once the task has checked for a stop request or waited for one, which shows that the task
   * can stop on its own.
   */
  @Volatile
  var isWatched = false
    private set

  val isRequested: Boolean
    get() = requested.count == 0L

  /** Returns true to the first caller only, so the SDK can take at most one signal for the task. */
  fun claim(): Boolean = claimed.compareAndSet(false, true)

  fun request() = requested.countDown()

  /**
   * Returns [isRequested] for the task. Because the task has now checked, this call also sets
   * [isWatched].
   */
  fun check(): Boolean {
    watch()
    return isRequested
  }

  fun await(timeout: Duration): Boolean {
    watch()
    return requested.await(TimeUnit.NANOSECONDS.convert(timeout), TimeUnit.NANOSECONDS)
  }

  private fun watch() {
    if (!isWatched) isWatched = true
  }

  companion object {
    /**
     * The stop request of the task that the server is running in this JVM. The server sets this field
     * before the task starts. When the server is done with the task, it sets the field back to null.
     * Every [org.apache.airflow.sdk.Context] made while the task runs keeps this instance, so a copy
     * of a context made during the task also sees the same request. A single field is enough because
     * the server runs one task at a time in a JVM, and a stop signal reaches the whole JVM, not one
     * task.
     */
    @Volatile
    var running: Cancellation? = null

    /**
     * Returns [running]. When the server is not running a task, for example in a unit test that calls
     * a task directly, it returns a new [Cancellation] that nobody asks to stop.
     */
    fun current(): Cancellation = running ?: Cancellation()
  }
}

private val logger = Logger(Cancellation::class)

/**
 * Opens a trap that passes SIGINT and SIGTERM to [onSignal] instead of the JVM. The trap stays open
 * until the caller closes the returned handle.
 *
 * When [onSignal] returns false or throws, the JVM gets its own handler back and receives the
 * signal again, so the JVM handles the signal as it would have without the trap. Only one trap can
 * be open at a time, because closing a trap puts back the handlers that were installed when that
 * trap opened.
 */
internal fun trapStopSignals(onSignal: (signal: String) -> Boolean): AutoCloseable {
  val restores = listOf("INT", "TERM").mapNotNull { trapSignal(it, onSignal) }
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
