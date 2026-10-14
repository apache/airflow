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

// "native" is a Java keyword, so the native Dags live in "nativedag".
package org.apache.airflow.e2e.nativedag;

import static org.apache.airflow.e2e.NativeBundleBuilder.QUEUE;

import java.util.List;
import org.apache.airflow.sdk.*;

/** The Dag the e2e's other native Dags trigger, so they can't trigger each other in a loop. */
public class TargetDag {
  public static class Receive implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom("triggered");
    }
  }

  public static DagDef build() {
    var dag =
        new DagDef("java_native_target_e2e")
            .config("description", "Native Java Dag the e2e's other native Dags trigger")
            .config("catchup", false)
            .config("tags", List.of("java-sdk", "native", "e2e"));
    dag.task("receive", Receive.class).config("queue", QUEUE);
    return dag;
  }
}
