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

// The bundle entry point: one bundle, two Python-owned Dags.
//
// Both Dags in `dags/` are declared in Python with `@task.stub` tasks routed to the Node
// coordinator, so this side only supplies the task bodies.

import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

import { buildSummaryMessage, report, summarize } from "./taskflow.js";

export async function buildMessage() {
  const client = getClient();
  const upstream = await client.getXCom<string>({
    key: "return_value",
    taskId: "python_start",
  });
  const greeting = await client.getVariable("typescript_example_greeting");
  const message = `${greeting ?? "hello from TypeScript"}; upstream=${upstream ?? "missing"}`;

  await client.setXCom({ key: "typescript_message", value: message });

  return {
    message,
    upstream,
  };
}

export async function readConnection() {
  const connection = await getClient().getConnection("typescript_example_http");

  return {
    id: connection?.id ?? null,
    type: connection?.type ?? null,
    host: connection?.host ?? null,
    login: connection?.login ?? null,
    hasPassword: connection?.password != null,
  };
}

// One register call lists everything this bundle provides.
// `build_message` appears under both Dags: two different handlers, told apart by the dag_id each
// is bound to and never by the task_id alone.
const bundle = new Bundle();
bundle.register(
  new TaskHandler("typescript_example", "build_message", buildMessage),
  new TaskHandler("typescript_example", "read_connection", readConnection),
  new TaskHandler("typescript_taskflow_example", "summarize", summarize),
  new TaskHandler("typescript_taskflow_example", "report", report),
  new TaskHandler("typescript_taskflow_example", "build_message", buildSummaryMessage),
);
await bundle.serve();
