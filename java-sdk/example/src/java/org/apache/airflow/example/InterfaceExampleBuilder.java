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

import java.util.Date;
import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("DuplicatedCode")
public class InterfaceExampleBuilder {
  private static final Logger logger = LoggerFactory.getLogger(InterfaceExampleBuilder.class);

  public static class Extract implements Task {
    public void execute(@NotNull Context context, Client client) throws Exception {
      logger.info("Hello from task");

      var pythonInput = client.getXCom("python_task_1");
      logger.info("Got XCom from Python Task 'python_task_1' {}", pythonInput);

      var connection = client.getConnection("test_http");
      logger.info("Got con {}", connection);

      for (var i = 0; i < 3; i++) {
        logger.info("Beep {}, next time will be {}", i, new Date());
        Thread.sleep(2 * 1000);
      }

      client.setXCom(new Date().getTime());
      logger.info("Goodbye from task");
    }
  }

  public static class Transform implements Task {
    public void execute(@NotNull Context context, Client client) {
      var extracted = client.getXCom("extract");
      logger.info("Got XCom from 'extract' {}", extracted);

      var variable = client.getVariable("my_variable");
      logger.info("Got variable {}", variable);

      logger.info("Push XCom to python task 2");
      client.setXCom(new Date().getTime());
    }
  }

  public static class Load implements Task {
    public void execute(@NotNull Context context, Client client) {
      var transformed = client.getXCom("transform");
      logger.info("Got XCom from 'transform' {}", transformed);
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
