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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Builder.Task(id = "transform")
  public long transformValue(Client client, @Builder.XCom(task = "extract") long extracted) {
    logger.info("Got XCom from 'extract' {}", extracted);

    var variable = client.getVariable("my_variable");
    logger.info("Got variable {}", variable);

    logger.info("Push XCom to python task 2");
    return new Date().getTime();
  }

  // load fails on its first attempt and succeeds on the retry. With retries
  // configured on the stub task, the first failure makes the supervisor mark
  // the task UP_FOR_RETRY -- which only works because the Java SDK now returns
  // RetryTask (instead of a terminal FAILED) when ti_context.should_retry is
  // set. The retry then runs this task again and it returns normally.
  @Builder.Task
  public void load(Context context, @Builder.XCom(task = "transform") long transformed) {
    logger.info("Got XCom from 'transform' {}", transformed);
    if (context.ti.tryNumber == 1) {
      throw new RuntimeException("I failed");
    }
    logger.info("Recovered on retry, try number {}", context.ti.tryNumber);
  }
}
