/*!
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

// TypeScript half of the KubernetesExecutor lang-SDK system test bundle. Registers the
// TypeScript tasks of the shared "lang_sdk_mixed_language" Dag; the Go and Java tasks of
// the same dag_id live in ../go_example, ../java_example and the Python stub Dag in
// ../dags. The coordinator locates this bundle by dag_id, so only the TypeScript tasks
// are registered here.

import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

const MIXED_LANGUAGE_DAG_ID = "lang_sdk_mixed_language";

export async function tsExtract() {
  return {
    node_version: process.version,
    timestamp: Date.now(),
  };
}

export async function tsTransform() {
  const value = await getClient().getVariable("my_variable");
  console.log(`ts_transform obtained variable: ${value}`);
}

const bundle = new Bundle();
bundle.register(
  new TaskHandler(MIXED_LANGUAGE_DAG_ID, "ts_extract", tsExtract),
  new TaskHandler(MIXED_LANGUAGE_DAG_ID, "ts_transform", tsTransform),
);
await bundle.serve();
