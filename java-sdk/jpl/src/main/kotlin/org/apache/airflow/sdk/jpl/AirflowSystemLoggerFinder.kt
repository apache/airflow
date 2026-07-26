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

package org.apache.airflow.sdk.jpl

import org.apache.airflow.sdk.execution.Level
import org.apache.airflow.sdk.execution.Log
import java.text.MessageFormat
import java.util.MissingResourceException
import java.util.ResourceBundle

/**
 * [System.LoggerFinder] that routes [System.Logger] calls to the Airflow task log store.
 *
 * Registered via [META-INF/services/java.lang.System$LoggerFinder][System.LoggerFinder].
 * All loggers share a single implementation regardless of the requesting module.
 */
class AirflowSystemLoggerFinder : System.LoggerFinder() {
  override fun getLogger(
    name: String,
    module: Module,
  ): System.Logger = AirflowSystemLogger(name)
}

internal class AirflowSystemLogger(
  private val name: String,
) : System.Logger {
  override fun getName(): String = name

  override fun isLoggable(level: System.Logger.Level): Boolean =
    level != System.Logger.Level.OFF && Log.isEnabledForLevel(level.convert(), name)

  override fun log(
    level: System.Logger.Level,
    bundle: ResourceBundle?,
    msg: String?,
    thrown: Throwable?,
  ) {
    if (!isLoggable(level)) return
    Log.send(level.convert(), name, bundle.resolve(msg)) {
      thrown?.let { put("exception", it.stackTraceToString()) }
    }
  }

  override fun log(
    level: System.Logger.Level,
    bundle: ResourceBundle?,
    format: String?,
    params: Array<out Any?>?,
  ) {
    if (!isLoggable(level)) return

    var message = bundle.resolve(format)

    fun renderEvent(): Pair<String, Map<String, Any?>> {
      if (params.isNullOrEmpty()) return message to emptyMap()
      val arguments =
        buildMap {
          message =
            try {
              MessageFormat.format(message, *params)
            } catch (e: IllegalArgumentException) {
              params.forEachIndexed { i, v -> put(i.toString(), v) }
              put("exception", e.stackTraceToString())
              message
            }
        }
      return message to arguments
    }

    val (event, arguments) = renderEvent()
    Log.send(level.convert(), name, event, arguments)
  }
}

private fun System.Logger.Level.convert(): Level =
  when (this) {
    System.Logger.Level.OFF, System.Logger.Level.ERROR -> Level.ERROR
    System.Logger.Level.WARNING -> Level.WARNING
    System.Logger.Level.INFO -> Level.INFO
    System.Logger.Level.DEBUG -> Level.DEBUG
    System.Logger.Level.TRACE, System.Logger.Level.ALL -> Level.NOTSET
  }

private fun ResourceBundle?.resolve(key: String?): String {
  if (key == null) return ""
  if (this == null) return key
  return try {
    getString(key)
  } catch (_: MissingResourceException) {
    key
  }
}
