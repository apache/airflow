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

/**
 * A snapshot of a single log message captured by [LogCapture.drain].
 *
 * This is a public-facing copy of the internal [LogMessage] type, suitable for
 * use in test assertions across Gradle module boundaries.
 */
data class CapturedLogMessage(
  val level: Level,
  val loggerName: String,
  val event: String,
  val arguments: Map<String, Any?>,
)

/**
 * Test utility for inspecting messages that were sent through [Log].
 *
 * When no channel is configured (the normal state during unit tests), [LogSender]
 * buffers every [Log.send] call in memory. [drain] returns those buffered messages
 * and clears the queue so successive calls are independent.
 *
 * Typical usage:
 * ```kotlin
 * @BeforeEach fun setUp() { LogCapture.drain() }  // clear any noise
 *
 * @Test fun `some test`() {
 *     myLogger.info("hello")
 *     val msg = LogCapture.drain().single()
 *     assertEquals(Level.INFO, msg.level)
 * }
 * ```
 */
object LogCapture {
  /**
   * Returns all messages buffered since the last call and clears the internal
   * queue.
   */
  fun drain(): List<CapturedLogMessage> =
    buildList {
      var msg = LogSender.messages.poll()
      while (msg != null) {
        add(CapturedLogMessage(msg.level, msg.logger, msg.event, msg.arguments))
        msg = LogSender.messages.poll()
      }
    }
}
