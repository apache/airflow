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

package org.apache.airflow.sdk.slf4j

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import org.slf4j.ILoggerFactory
import org.slf4j.IMarkerFactory
import org.slf4j.Logger
import org.slf4j.Marker
import org.slf4j.helpers.AbstractLogger
import org.slf4j.helpers.BasicMarkerFactory
import org.slf4j.helpers.MessageFormatter
import org.slf4j.helpers.NOPMDCAdapter
import org.slf4j.spi.MDCAdapter
import org.slf4j.spi.SLF4JServiceProvider
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.event.Level as SLevel

private fun SLevel.convert(): Level =
  when (this) {
    SLevel.TRACE -> Level.NOTSET
    SLevel.DEBUG -> Level.DEBUG
    SLevel.INFO -> Level.INFO
    SLevel.WARN -> Level.WARNING
    SLevel.ERROR -> Level.ERROR
  }

internal class AirflowSlf4jLogger(
  name: String,
) : AbstractLogger() {
  init {
    this.name = name
  }

  override fun getFullyQualifiedCallerName(): String? = null

  override fun handleNormalizedLoggingCall(
    level: SLevel,
    marker: Marker?,
    messagePattern: String?,
    arguments: Array<out Any?>?,
    throwable: Throwable?,
  ) {
    val event = MessageFormatter.basicArrayFormat(messagePattern ?: "", arguments)
    Log.send(level.convert(), name, event) {
      throwable?.run { put("exception", stackTraceToString()) }
      marker?.let { put("marker", it.name) }
    }
  }

  override fun isTraceEnabled(marker: Marker?) = Log.isEnabledForLevel(Level.NOTSET, name)

  override fun isTraceEnabled() = Log.isEnabledForLevel(Level.NOTSET, name)

  override fun isDebugEnabled(marker: Marker?) = Log.isEnabledForLevel(Level.DEBUG, name)

  override fun isDebugEnabled() = Log.isEnabledForLevel(Level.DEBUG, name)

  override fun isInfoEnabled(marker: Marker?) = Log.isEnabledForLevel(Level.INFO, name)

  override fun isInfoEnabled() = Log.isEnabledForLevel(Level.INFO, name)

  override fun isWarnEnabled(marker: Marker?) = Log.isEnabledForLevel(Level.WARNING, name)

  override fun isWarnEnabled() = Log.isEnabledForLevel(Level.WARNING, name)

  override fun isErrorEnabled(marker: Marker?) = Log.isEnabledForLevel(Level.ERROR, name)

  override fun isErrorEnabled() = Log.isEnabledForLevel(Level.ERROR, name)
}

internal class AirflowLoggerFactory : ILoggerFactory {
  private val loggers = ConcurrentHashMap<String, AirflowSlf4jLogger>()

  override fun getLogger(name: String): Logger = loggers.computeIfAbsent(name) { AirflowSlf4jLogger(it) }
}

class AirflowSlf4jProvider : SLF4JServiceProvider {
  private lateinit var factory: AirflowLoggerFactory

  override fun getLoggerFactory(): ILoggerFactory = factory

  override fun getMarkerFactory(): IMarkerFactory = BasicMarkerFactory()

  override fun getMDCAdapter(): MDCAdapter = NOPMDCAdapter()

  override fun getRequestedApiVersion() = REQUESTED_SLF4J_API_VERSION

  override fun initialize() {
    factory = AirflowLoggerFactory()
  }
}
