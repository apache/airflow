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

package org.apache.airflow.sdk.log4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.airflow.sdk.execution.Level;
import org.apache.airflow.sdk.execution.Log;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.spi.StandardLevel;

/**
 * A Log4j {@link Appender} to route logs to Airflow.
 *
 * <p>This class is not called explicitly. An annotation processor reads the
 * class (since it's annotated with {@link Plugin}) and generates information
 * needed by Log4J.
 */
@Plugin(
    name = "AirflowAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE)
public final class AirflowLog4jAppender extends AbstractAppender {

  private AirflowLog4jAppender(String name, Filter filter) {
    super(name, filter, null, true, Property.EMPTY_ARRAY);
  }

  @PluginFactory
  public static AirflowLog4jAppender createAppender(
      @PluginAttribute("name") String name, @PluginElement("Filter") Filter filter) {
    return new AirflowLog4jAppender(name == null ? "AirflowAppender" : name, filter);
  }

  @Override
  public void append(LogEvent event) {
    var level = convert(event.getLevel());
    var logger = event.getLoggerName();
    if (!Log.INSTANCE.isEnabledForLevel(level, logger)) return;
    // Log4J does not really provide a good way to access the underlying unformatted data
    // since it allows vastly different logging mechanisms. We pre-format the message here
    // and send the exception and marker separately as structured metadata.
    String message = event.getMessage().getFormattedMessage();
    Map<String, Object> args = new HashMap<>();
    var thrown = event.getThrown();
    if (thrown != null) {
      args.put("exception", stackTrace(thrown));
    }
    var marker = event.getMarker();
    if (marker != null) {
      args.put("marker", marker.getName());
    }
    Log.INSTANCE.send(level, logger, message, args);
  }

  private static Level convert(org.apache.logging.log4j.Level level) {
    var v = level.intLevel();
    if (v < StandardLevel.ERROR.intLevel()) return Level.CRITICAL;
    if (v < StandardLevel.WARN.intLevel()) return Level.ERROR;
    if (v < StandardLevel.INFO.intLevel()) return Level.WARNING;
    if (v < StandardLevel.DEBUG.intLevel()) return Level.INFO;
    if (v < StandardLevel.TRACE.intLevel()) return Level.DEBUG;
    return Level.NOTSET;
  }

  private static String stackTrace(Throwable t) {
    var sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
