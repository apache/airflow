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

import org.apache.airflow.sdk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@SuppressWarnings("DuplicatedCode")
@Builder.Dag(id = "java_annotation_example")
public class AnnotationExample {
  private static final Logger logger = LoggerFactory.getLogger(AnnotationExample.class);

  @Builder.Task(id = "extract")
  public long extractValue(Client client) throws InterruptedException {
    logger.info("Hello from task");

    var pythonXcom = client.getXCom("python_task_1");
    logger.info("Got XCom from Python Task 'python_task_1' {}", pythonXcom);

    var connection = client.getConnection("test_http");
    logger.info("Got con {}", connection);

    for (var i = 0; i < 3; i++) {
      logger.info("Beep {}, next time will be {}", i, new Date());
      Thread.sleep(2 * 1000);
    }

    logger.info("Goodbye from task");
    return new Date().getTime();
  }

  @Builder.Task(id = "transform", depends = {"extract"})
  public long transformValue(Client client, @Builder.XCom(task = "extract") long extracted) {
    logger.info("Got XCom from 'extract' {}", extracted);

    var variable = client.getVariable("my_variable");
    logger.info("Got variable {}", variable);

    logger.info("Push XCom to python task 2");
    return new Date().getTime();
  }

  @Builder.Task(depends = {"transform"})
  public void load(@Builder.XCom(task = "transform") long transformed) {
    logger.info("Got XCom from 'transform' {}", transformed);
    throw new RuntimeException("I failed");
  }
}
