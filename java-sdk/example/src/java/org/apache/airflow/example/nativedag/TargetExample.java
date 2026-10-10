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

// The Dag the other two native examples trigger. It triggers nothing itself,
// so the examples cannot trigger each other in a loop.
public class TargetExample {
  private static final System.Logger log = System.getLogger(TargetExample.class.getName());

  public static class Receive implements Task {
    @Override
    public void execute(Context context, Client client) {
      log.log(INFO, "Triggered by another Dag");
    }
  }

  public static DagDef build() {
    var dag =
        new DagDef("java_native_target_example")
            .config("description", "Pure-Java Dag that the other native examples trigger")
            .config("queue", "java")
            .config("catchup", false)
            .config("tags", List.of("example", "java-sdk"));
    dag.task("receive", Receive.class);
    return dag;
  }
}
