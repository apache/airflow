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

import org.slf4j.ILoggerFactory
import org.slf4j.IMarkerFactory
import org.slf4j.Marker
import org.slf4j.helpers.AbstractLogger
import org.slf4j.helpers.BasicMDCAdapter
import org.slf4j.helpers.BasicMarkerFactory
import org.slf4j.helpers.MessageFormatter
import org.slf4j.spi.MDCAdapter
import org.slf4j.spi.SLF4JServiceProvider
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.Logger as Slf4jLogger
import org.slf4j.event.Level as Slf4jLevel

/**
 * SLF4J binding that streams task logs through the Airflow logs socket.
 *
 * Without a binding, an application's SLF4J calls fall back to whatever
 * provider is on the classpath (e.g. slf4j-simple writing to stderr), and the
 * supervisor then tags every stderr line as ERROR regardless of the original
 * level. Routing logs through [LogSender] instead carries each message's real
 * level on the wire, so the UI shows INFO/WARN/etc. as logged.
 */
class AirflowSlf4jServiceProvider : SLF4JServiceProvider {
  private val loggerFactory = AirflowLoggerFactory()
  private val markerFactory: IMarkerFactory = BasicMarkerFactory()
  private val mdcAdapter: MDCAdapter = BasicMDCAdapter()

  override fun getLoggerFactory(): ILoggerFactory = loggerFactory

  override fun getMarkerFactory(): IMarkerFactory = markerFactory

  override fun getMDCAdapter(): MDCAdapter = mdcAdapter

  override fun getRequestedApiVersion(): String = "2.0.99"

  override fun initialize() {}
}

internal class AirflowLoggerFactory : ILoggerFactory {
  private val loggers = ConcurrentHashMap<String, Slf4jLogger>()

  override fun getLogger(name: String): Slf4jLogger = loggers.computeIfAbsent(name, ::AirflowSlf4jLogger)
}

internal class AirflowSlf4jLogger(
  private val loggerName: String,
) : AbstractLogger() {
  init {
    name = loggerName
  }

  // The supervisor delegates threshold filtering to the subprocess (it sets
  // level_override=NOTSET), so drop sub-threshold events here rather than forward
  // everything. See [LogLevel].
  override fun isTraceEnabled(): Boolean = LogLevel.isEnabled(Level.TRACE)

  override fun isTraceEnabled(marker: Marker?): Boolean = LogLevel.isEnabled(Level.TRACE)

  override fun isDebugEnabled(): Boolean = LogLevel.isEnabled(Level.DEBUG)

  override fun isDebugEnabled(marker: Marker?): Boolean = LogLevel.isEnabled(Level.DEBUG)

  override fun isInfoEnabled(): Boolean = LogLevel.isEnabled(Level.INFO)

  override fun isInfoEnabled(marker: Marker?): Boolean = LogLevel.isEnabled(Level.INFO)

  override fun isWarnEnabled(): Boolean = LogLevel.isEnabled(Level.WARN)

  override fun isWarnEnabled(marker: Marker?): Boolean = LogLevel.isEnabled(Level.WARN)

  override fun isErrorEnabled(): Boolean = LogLevel.isEnabled(Level.ERROR)

  override fun isErrorEnabled(marker: Marker?): Boolean = LogLevel.isEnabled(Level.ERROR)

  override fun getFullyQualifiedCallerName(): String? = null

  override fun handleNormalizedLoggingCall(
    level: Slf4jLevel,
    marker: Marker?,
    messagePattern: String?,
    arguments: Array<out Any?>?,
    throwable: Throwable?,
  ) {
    val airflowLevel = level.toAirflowLevel()
    if (!LogLevel.isEnabled(airflowLevel)) return
    @Suppress("UNCHECKED_CAST")
    val message = MessageFormatter.basicArrayFormat(messagePattern, arguments as Array<Any?>?)
    val extra = throwable?.let { mapOf<String, Any>("error_detail" to it.stackTraceToString()) } ?: emptyMap()
    LogSender.send(LogMessage(message ?: "", extra, loggerName, airflowLevel))
  }
}

internal fun Slf4jLevel.toAirflowLevel(): Level =
  when (this) {
    Slf4jLevel.TRACE -> Level.TRACE
    Slf4jLevel.DEBUG -> Level.DEBUG
    Slf4jLevel.INFO -> Level.INFO
    Slf4jLevel.WARN -> Level.WARN
    Slf4jLevel.ERROR -> Level.ERROR
  }
