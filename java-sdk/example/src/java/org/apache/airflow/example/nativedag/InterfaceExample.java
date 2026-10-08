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

// A Dag defined entirely in Java, interface-style. dag.task registers a task as
// it creates it and hands back the handle, and `before`/`after` wire the graph.
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

  public static class LoadEmpty implements Task {
    @Override
    public void execute(Context context, Client client) {
      log.log(INFO, "Nothing to load");
    }
  }

  // A condition: its boolean picks one of the two loads, and the other is
  // skipped.
  public static class HasRows implements ConditionTask {
    @Override
    public boolean decide(Context context, Client client) {
      return ((Number) client.getXCom("transform")).longValue() > 0;
    }
  }

  public static class ReportLong implements Task {
    @Override
    public void execute(Context context, Client client) {
      log.log(INFO, "Long report");
    }
  }

  public static class ReportShort implements Task {
    @Override
    public void execute(Context context, Client client) {
      log.log(INFO, "Short report");
    }
  }

  // A branch: it names the one case that runs by its class, and every other
  // case is skipped.
  public static class PickReport implements BranchTask {
    @Override
    public Class<? extends Task> choose(Context context, Client client) {
      return ((Number) client.getXCom("transform")).longValue() > 100
          ? ReportLong.class
          : ReportShort.class;
    }
  }

  public static DagDef build() {
    var dag =
        new DagDef("java_native_interface_example")
            .config("description", "Pure-Java Dag authored with the interface API")
            .config("schedule", "@daily")
            .config("catchup", false)
            .config("tags", List.of("example", "java-sdk"));

    var extract =
        dag.task("extract", Extract.class)
            .config("retries", 2)
            .config("doc_md", "Extracts a value and pushes it as an XCom.");
    var transform = dag.task("transform", Transform.class);
    var load = dag.task("load", Load.class);
    var loadEmpty = dag.task("load_empty", LoadEmpty.class);

    var reportLong = dag.task("report_long", ReportLong.class);
    var reportShort = dag.task("report_short", ReportShort.class);

    transform.after(extract);
    // With no task id given, a decider takes one from its class: "hasRows" and
    // "pickReport".
    dag.If(HasRows.class).after(transform).then(load).orElse(loadEmpty);
    dag.Branch(PickReport.class).after(transform).option(reportLong).option(reportShort);
    return dag;
  }
}
