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

package org.apache.airflow.example;

import static java.lang.System.Logger.Level.INFO;

import java.util.Date;
import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("DuplicatedCode")
public class InterfaceExampleBuilder {
  private static final System.Logger log =
      System.getLogger(InterfaceExampleBuilder.class.getName());

  public static class Extract implements Task {
    public void execute(@NotNull Context context, Client client) throws Exception {
      log.log(INFO, "Hello from task");

      var pythonInput = client.getXCom("python_task_1");
      log.log(INFO, "Got XCom from python_task_1: {0}", pythonInput);

      var connection = client.getConnection("test_http");
      log.log(INFO, "Got connection: {0}", connection);

      for (var i = 0; i < 3; i++) {
        log.log(INFO, "Beep {0}, next time will be {1}", i, new Date());
        Thread.sleep(2 * 1000);
      }

      client.setXCom(new Date().getTime());
      log.log(INFO, "Goodbye from task");
    }
  }

  public static class Transform implements Task {
    public void execute(@NotNull Context context, Client client) {
      var extracted = client.getXCom("extract");
      log.log(INFO, "Got XCom from extract: {0}", extracted);

      var variable = client.getVariable("my_variable");
      log.log(INFO, "Got variable: {0}", variable);

      log.log(INFO, "Push XCom to python task 2");
      client.setXCom(new Date().getTime());
    }
  }

  public static class Load implements Task {
    public void execute(@NotNull Context context, Client client) {
      var transformed = client.getXCom("transform");
      log.log(INFO, "Got XCom from transform: {0}", transformed);
      throw new RuntimeException("I failed");
    }
  }

  public static Dag build() {
    var dag = new Dag("java_interface_example");
    dag.addTask("extract", Extract.class);
    dag.addTask("transform", Transform.class);
    dag.addTask("load", Load.class);
    return dag;
  }
}
