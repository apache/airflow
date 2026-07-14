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

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.airflow.sdk.*;

@SuppressWarnings("DuplicatedCode")
@Builder.Dag(id = "java_annotation_example")
public class AnnotationExample {
  private static final System.Logger log = System.getLogger(AnnotationExample.class.getName());

  @Builder.Task(id = "extract")
  public long extractValue(Client client) throws InterruptedException {
    log.log(INFO, "Hello from task");

    var pythonXcom = client.getXCom("python_task_1");
    log.log(INFO, "Got XCom from python_task_1: {0}", pythonXcom);

    var connection = client.getConnection("test_http");
    log.log(INFO, "Got connection: {0}", connection);

    for (var i = 0; i < 3; i++) {
      log.log(INFO, "Beep {0}, next time will be {1}", i, new Date());
      Thread.sleep(2 * 1000);
    }

    log.log(INFO, "Goodbye from task");
    return new Date().getTime();
  }

  @Builder.Task(id = "transform")
  public long transformValue(Client client, @Builder.XCom(task = "extract") long extracted) {
    log.log(INFO, "Got XCom from extract: {0}", extracted);

    var variable = client.getVariable("my_variable");
    log.log(INFO, "Got variable: {0}", variable);

    log.log(INFO, "Push XCom to python task 2");
    return new Date().getTime();
  }

  // load fails on its first attempt and succeeds on the retry. With retries
  // configured on the stub task, the first failure makes the supervisor mark
  // the task UP_FOR_RETRY -- which only works because the Java SDK now returns
  // RetryTask (instead of a terminal FAILED) when ti_context.should_retry is
  // set. The retry then runs this task again and it returns normally.
  @Builder.Task
  public void load(Context context, @Builder.XCom(task = "transform") long transformed) {
    log.log(INFO, "Got XCom from transform: {0}", transformed);
    if (context.ti.tryNumber == 1) {
      throw new RuntimeException("I failed");
    }
    log.log(INFO, "Recovered on retry, try number {0}", context.ti.tryNumber);
  }

  // Verify one supervisor channel can handle client calls across threads.
  @Builder.Task(id = "concurrent")
  public void concurrentClientCalls(Client client) throws Exception {
    var pool = Executors.newFixedThreadPool(8);
    try {
      var calls = new ArrayList<Callable<String>>();
      for (var i = 0; i < 32; i++) {
        calls.add(() -> client.getConnection("test_http").host);
      }
      for (var future : pool.invokeAll(calls)) {
        var host = future.get();
        if (!"example.com".equals(host)) {
          throw new RuntimeException("concurrent getConnection returned wrong host: " + host);
        }
      }
      log.log(INFO, "All concurrent client calls returned the correct connection");
    } finally {
      pool.shutdown();
    }
  }
}
