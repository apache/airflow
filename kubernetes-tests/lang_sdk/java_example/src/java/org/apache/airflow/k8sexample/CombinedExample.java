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

package org.apache.airflow.k8sexample;

import java.util.Date;
import org.apache.airflow.sdk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Java half of the KubernetesExecutor lang-SDK system test bundle. Registers the
// Java tasks of the shared "lang_sdk_combined" Dag; the Go tasks of the same
// dag_id live in ../go_example and the Python stub Dag in ../dags. The
// coordinator locates this jar by dag_id, so only the Java tasks are registered
// here.
@Builder.Dag(id = "lang_sdk_combined")
public class CombinedExample {
  private static final Logger logger = LoggerFactory.getLogger(CombinedExample.class);

  @Builder.Task(id = "java_extract")
  public long extract(Client client) {
    logger.info("java_extract running");
    return new Date().getTime();
  }

  @Builder.Task(id = "java_transform")
  public void transform(Client client) {
    var variable = client.getVariable("my_variable");
    logger.info("java_transform obtained variable {}", variable);
  }
}
