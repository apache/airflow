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

import org.apache.airflow.sdk.*;

// A Dag defined entirely in Java, interface-style: no Python stub file
// describes it. dag.task registers a task as it creates it and hands back the
// handle, and `before`/`after` wire the graph -- Java's spelling of `>>` and `<<`.
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
    var dag = new DagDef("java_native_interface_example");

    var extract = dag.task("extract", Extract.class);
    var transform = dag.task("transform", Transform.class);
    var load = dag.task("load", Load.class);

    transform.after(extract).before(load);
    return dag;
  }
}
