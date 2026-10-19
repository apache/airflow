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

import org.apache.airflow.sdk.*;

// The bundle holds task handlers only: the "lang_sdk_mixed_language" Dag is the Python
// file's, so nothing here declares a Dag.
public class K8sBundleBuilder {
  public static Bundle build() {
    return new Bundle().register(MixedLanguageExample.class);
  }

  public static void main(String[] args) {
    Server.create(args).serve(build());
  }
}
