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

package org.apache.airflow.e2e;

import org.apache.airflow.sdk.*;

/**
 * Task handler whose stub Dag calls it with one argument more than it declares.
 *
 * Registered with {@code Bundle.register(Class)} rather than {@code
 * TestBundleBuilder}'s hand-written {@code Task} classes, so the generated {@code
 * TaskParams} gives it a positional binding (see {@code java_task_handler_failures.py}).
 */
public class TaskHandlerFailures {
  private static final System.Logger log = System.getLogger(TaskHandlerFailures.class.getName());

  @Builder.TaskHandler(dag = "java_task_handler_failures", task = "takes_two_numbers")
  public void takesTwoNumbers(Context context, long first, long second) {
    log.log(System.Logger.Level.INFO, "Took two numbers: {0}, {1}", first, second);
  }
}
