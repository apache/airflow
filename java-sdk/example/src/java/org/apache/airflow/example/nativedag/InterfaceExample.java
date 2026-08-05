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

// "native" is a Java keyword, so the native-Dag examples live in "nativedag".
package org.apache.airflow.example.nativedag;

import static java.lang.System.Logger.Level.INFO;

import java.util.List;
import org.apache.airflow.sdk.*;

// A Dag defined entirely in Java, interface-style: no Python stub file
// describes it. TaskDef.config carries the task configuration, dependsOn
// declares the graph, and DagDef.config carries the Dag configuration.
public class InterfaceExample {
  private static final System.Logger log = System.getLogger(InterfaceExample.class.getName());

  public static class Extract implements Task {
    @Override
    public void execute(Context context, Client client) {
      log.log(INFO, "Extracting a value");
      client.setXCom(42L);
    }
  }

  public static class Transform implements Task {
    @Override
    public void execute(Context context, Client client) {
      var extracted = ((Number) client.getXCom("extract")).longValue();
      log.log(INFO, "Transforming {0}", extracted);
      client.setXCom(extracted * 2);
    }
  }

  public static class Load implements Task {
    @Override
    public void execute(Context context, Client client) {
      var transformed = client.getXCom("transform");
      log.log(INFO, "Loaded {0}", transformed);
    }
  }

  public static DagDef build() {
    var extract =
        new TaskDef("extract", Extract.class)
            .config("retries", 2)
            .config("doc_md", "Extracts a value and pushes it as an XCom.");
    var transform = new TaskDef("transform", Transform.class).dependsOn(extract);
    var load = new TaskDef("load", Load.class).dependsOn(transform);
    return new DagDef("java_native_interface_example")
        .config(
            "description",
            "Pure-Java Dag authored with the interface API, without a Python stub file")
        .config("schedule", "@daily")
        .config("catchup", false)
        .config("tags", List.of("example", "java-sdk"))
        .addTask(extract)
        .addTask(transform)
        .addTask(load);
  }
}
