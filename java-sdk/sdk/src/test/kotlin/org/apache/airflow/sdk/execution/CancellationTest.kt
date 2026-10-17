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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import sun.misc.Signal
import sun.misc.SignalHandler
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Runs [block] while the named [signals], such as "TERM", go to a recorder instead of the JVM's own
 * handlers. A signal that the code under test fails to catch then fails an assertion instead of
 * stopping the test JVM.
 */
internal fun withSignalRecorder(
  vararg signals: String,
  block: (recorded: BlockingQueue<String>) -> Unit,
) {
  val recorded = LinkedBlockingQueue<String>()
  val originals =
    signals.associate { name ->
      val signal = Signal(name)
      signal to Signal.handle(signal) { recorded.put("SIG$name") }
    }
  try {
    // The OS never delivers an ignored signal, so a test that raises one cannot pass.
    Assumptions.assumeTrue(SignalHandler.SIG_IGN !in originals.values, "a signal this test raises is ignored in this JVM")
    block(recorded)
  } finally {
    originals.forEach { (signal, handler) -> Signal.handle(signal, handler) }
  }
}

/** Waits for the next [count] signal names that reach [queue]. */
internal fun pollSignals(
  queue: BlockingQueue<String>,
  count: Int,
): List<String?> = List(count) { queue.poll(10, TimeUnit.SECONDS) }

/**
 * Main class of the JVMs that [CancellationTest] starts.
 *
 * Without arguments, it prints each signal that trapStopSignals cannot catch. With `give-back`, it
 * raises SIGTERM in a trap whose callback declines the signal. With `hold`, it raises SIGTERM in a
 * trap whose callback takes the signal. In both modes it prints what happens next.
 */
object TrapProbe {
  @JvmStatic
  fun main(args: Array<String>) {
    when (args.singleOrNull()) {
      null -> {
        trapStopSignals { false }.close()
        LogCapture
          .drain()
          .filter { it.event == "Cannot catch signal, so the task cannot see it" }
          .forEach { println("${it.arguments["signal"]} ${it.arguments["error"]?.javaClass?.name}") }
      }
      else -> {
        val take = args.single() == "hold"
        Runtime.getRuntime().addShutdownHook(Thread { println("shutdown hook ran") })
        trapStopSignals { take }.use {
          Signal.raise(Signal("TERM"))
          Thread.sleep(2000)
          println("still running")
        }
      }
    }
  }
}

class CancellationTest {
  @Test
  @DisplayName("Should pass SIGINT and SIGTERM to the callback until closed, then hand them back")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun trapsStopSignalsUntilClosed() =
    withSignalRecorder("INT", "TERM") { recorded ->
      val trapped = LinkedBlockingQueue<String>()
      trapStopSignals {
        trapped.put(it)
        true
      }.use {
        Signal.raise(Signal("INT"))
        Signal.raise(Signal("TERM"))
        Assertions.assertEquals(setOf("SIGINT", "SIGTERM"), pollSignals(trapped, 2).toSet(), "the callback should get both signals")
      }
      Signal.raise(Signal("INT"))
      Signal.raise(Signal("TERM"))
      Assertions.assertEquals(
        setOf("SIGINT", "SIGTERM"),
        pollSignals(recorded, 2).toSet(),
        "closing the trap should hand both signals back to the handlers that were there before",
      )
      Assertions.assertTrue(trapped.isEmpty(), "the callback should not get signals after the trap is closed")
    }

  @Test
  @DisplayName("Should hand a signal the callback declines back to the previous handler, and keep later ones there")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun handsBackDeclinedSignal() =
    withSignalRecorder("TERM") { recorded ->
      val offered = LinkedBlockingQueue<String>()
      trapStopSignals {
        offered.put(it)
        false
      }.use {
        Signal.raise(Signal("TERM"))
        Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "a declined signal should reach the previous handler")
        Signal.raise(Signal("TERM"))
        Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "after declining, the trap should leave SIGTERM alone")
      }
      Assertions.assertEquals(listOf("SIGTERM"), offered.toList(), "only the first signal should reach the callback")
    }

  @Test
  @DisplayName("Should hand a signal back to the previous handler when the callback throws")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun handsBackSignalWhenCallbackThrows() =
    withSignalRecorder("TERM") { recorded ->
      trapStopSignals { throw IllegalStateException("the log channel is closed") }.use {
        Signal.raise(Signal("TERM"))
        Assertions.assertEquals(listOf("SIGTERM"), pollSignals(recorded, 1), "a failing callback should not lose the signal")
      }
    }

  @Test
  @DisplayName("Should let the JVM run its shutdown hooks and exit with 143 when the callback declines SIGTERM")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun declinedSignalStopsTheJvm() {
    val (exitCode, output) = runTrapProbe("give-back")

    Assertions.assertEquals(143, exitCode, "the JVM should exit as it does on SIGTERM without a trap")
    Assertions.assertEquals(listOf("shutdown hook ran"), output)
  }

  @Test
  @DisplayName("Should keep the JVM running when the callback takes SIGTERM")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun takenSignalKeepsTheJvmRunning() {
    val (exitCode, output) = runTrapProbe("hold")

    Assertions.assertEquals(0, exitCode)
    Assertions.assertEquals(listOf("still running", "shutdown hook ran"), output, "the hook should run only at the normal exit")
  }

  @Test
  @DisplayName("Should warn and leave the signals alone when the JVM is started with -Xrs")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun warnsWhenTheJvmKeepsTheSignals() {
    Assertions.assertEquals(
      0 to listOf("SIGINT java.lang.IllegalArgumentException", "SIGTERM java.lang.IllegalArgumentException"),
      runTrapProbe(jvmOptions = listOf("-Xrs")),
    )
  }

  @Test
  @DisplayName("Should warn and leave the signals alone when the runtime has no jdk.unsupported module")
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  fun warnsWhenSunMiscSignalIsMissing() {
    Assertions.assertEquals(
      0 to listOf("SIGINT java.lang.NoClassDefFoundError", "SIGTERM java.lang.NoClassDefFoundError"),
      runTrapProbe(jvmOptions = listOf("--limit-modules", "java.base")),
    )
  }

  private fun runTrapProbe(
    vararg args: String,
    jvmOptions: List<String> = emptyList(),
  ): Pair<Int, List<String>> {
    val java =
      ProcessHandle
        .current()
        .info()
        .command()
        .orElseThrow()
    val process =
      ProcessBuilder(listOf(java) + jvmOptions + listOf("-cp", System.getProperty("java.class.path"), TrapProbe::class.java.name) + args)
        .redirectErrorStream(true)
        .start()
    val output = process.inputStream.bufferedReader().readLines()
    return process.waitFor() to output
  }
}
