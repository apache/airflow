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

package org.apache.airflow.sdk.jul

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import java.util.logging.Level as JLevel

/**
 * Convert a JUL Level to an SDK Level.
 *
 * JUL levels are VASTLY different from SDK levels. The `>` and `>=` criteria
 * are chosen intentionally (but also arbitrarily) to fit JUL level regions
 * more equally, while still keeping the predefined levels match.
 */
private fun JLevel.convert() =
  intValue().let {
    if (it > JLevel.SEVERE.intValue()) {
      Level.CRITICAL
    } else if (it > JLevel.WARNING.intValue()) {
      Level.ERROR
    } else if (it > JLevel.INFO.intValue()) {
      Level.WARNING
    } else if (it >= JLevel.CONFIG.intValue()) {
      Level.INFO
    } else if (it >= JLevel.FINER.intValue()) {
      Level.DEBUG
    } else {
      Level.NOTSET
    }
  }

/**
 * A [Handler] that routes java.util.logging records through the Airflow Java SDK's
 * log pipeline to Airflow's task log store.
 */
class AirflowJulHandler : Handler() {
  // Used only for [java.util.logging.Formatter.formatMessage]: it localizes
  // and substitutes  parameters but, unlike format(), never appends the
  // throwable's stack trace, which we send separately instead, to the text.
  private val formatter = SimpleFormatter()

  override fun publish(record: LogRecord) {
    if (!isLoggable(record)) return
    val level = record.level.convert()
    val logger = record.loggerName
    if (!Log.isEnabledForLevel(level, logger)) return
    Log.send(level, logger ?: "", formatter.formatMessage(record)) {
      record.thrown?.run { put("exception", stackTraceToString()) }
    }
  }

  override fun flush() = Unit

  override fun close() = Unit

  companion object {
    /**
     * Route java.util.logging through Airflow by installing a single
     * [AirflowJulHandler] on the root logger. Call this in the Dag bundle's
     * `main` method before you create the [org.apache.airflow.sdk.Bundle]
     * object; repeated calls are idempotent.
     *
     * By default it first removes the root logger's existing handlers, notably
     * the JDK's [java.util.logging.ConsoleHandler], which would otherwise also
     * write each record to stderr that Airflow captures separately as
     * `task.stderr` at ERROR level. Pass `clean = false` to instead add the
     * handler alongside existing ones (e.g. your own
     * [java.util.logging.FileHandler]); none of them must write to stderr, or
     * task logs are duplicated. For full control, use a `logging.properties`
     * file.
     *
     * @param clean remove the root logger's existing handlers first (default `true`).
     */
    @JvmStatic
    @JvmOverloads
    fun setup(clean: Boolean = true) {
      val root = Logger.getLogger("")
      if (clean) {
        root.handlers.forEach { root.removeHandler(it) }
      }
      if (root.handlers.none { it is AirflowJulHandler }) {
        root.addHandler(AirflowJulHandler())
      }
    }
  }
}
