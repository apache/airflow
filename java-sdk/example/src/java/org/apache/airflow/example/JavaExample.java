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
import java.util.List;
import org.apache.airflow.sdk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaExample implements DagBundle {
  private static final Logger logger = LoggerFactory.getLogger(JavaExample.class);

  public static class Extract implements Task {
    public void execute(Client client) throws Exception {
      logger.info("Hello from task");

      var python_xcom = client.getXCom("python_task_1");
      logger.info("Got XCom from Python Task 'python_task_1' {}", python_xcom);

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
    public void execute(Client client) {
      var extract_xcom = client.getXCom("extract");
      logger.info("Got XCom from 'extract' {}", extract_xcom);

      var variable = client.getVariable("my_variable");
      logger.info("Got variable {}", variable);

      logger.info("Push XCom to python task 2");
      client.setXCom(new Date().getTime());
    }
  }

  public static class Load implements Task {
    public void execute(Client client) {
      var xcom = client.getXCom("transform");
      logger.info("Got XCom from 'transform' {}", xcom);
      throw new RuntimeException("I failed");
    }
  }

  @Override
  public List<Dag> getDags() {
    var javaExample = new Dag("java_example", /* description= */ null, /* schedule= */ "@daily");
    javaExample.addTask("extract", Extract.class, List.of());
    javaExample.addTask("transform", Transform.class, List.of("extract"));
    javaExample.addTask("load", Load.class, List.of("transform"));
    return List.of(javaExample);
  }

  public static void main(String[] args) {
    var example = new JavaExample();
    var bundle =
        new Bundle(JavaExample.class.getPackage().getImplementationVersion(), example.getDags());

    Server.create(args).serve(bundle);
  }
}
