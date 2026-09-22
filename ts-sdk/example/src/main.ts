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

import { Bundle, getClient, getContext, NEVER_EXPIRE, TaskHandler } from "apache-airflow-ts-sdk";

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

/** Records the run that last wrote it, so a later run can see it changed. */
const LAST_RUN_VARIABLE = "typescript_example_last_run";
/** Written and deleted within the same task, to show both write directions. */
const SCRATCH_VARIABLE = "typescript_example_scratch";

export async function writeAndDeleteVariable() {
  const client = getClient();
  const { runId } = getContext();

  await client.setVariable(LAST_RUN_VARIABLE, runId, "Run id of the last typescript_example run");
  await client.setVariable(SCRATCH_VARIABLE, runId);
  await client.deleteVariable(SCRATCH_VARIABLE);
}

const JOB_ID_KEY = "typescript_example_job_id";
const STATE_SCRATCH_KEY = "typescript_example_scratch";

export async function writeAndReadTaskState() {
  const client = getClient();
  const { runId, tryNumber } = getContext();

  await client.setTaskStateStore({ key: JOB_ID_KEY, value: runId, retentionMs: NEVER_EXPIRE });
  await client.setTaskStateStore({ key: STATE_SCRATCH_KEY, value: { attempt: tryNumber } });

  const jobId = await client.getTaskStateStore<string>(JOB_ID_KEY);
  const scratchBeforeDelete = await client.getTaskStateStore(STATE_SCRATCH_KEY);

  await client.deleteTaskStateStore(STATE_SCRATCH_KEY);
  const scratchAfterDelete = await client.getTaskStateStore(STATE_SCRATCH_KEY);

  return { jobId, scratchBeforeDelete, scratchAfterDelete };
}

export async function clearTaskState() {
  const client = getClient();

  await client.setTaskStateStore({ key: JOB_ID_KEY, value: "placeholder" });
  await client.setTaskStateStore({ key: STATE_SCRATCH_KEY, value: { attempt: 1 } });

  await client.clearTaskStateStore();

  const afterClear = await client.getTaskStateStore(JOB_ID_KEY);

  return { afterClear };
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
  new TaskHandler("typescript_example", "write_and_delete_variable", writeAndDeleteVariable),
  new TaskHandler("typescript_example", "write_and_read_task_state", writeAndReadTaskState),
  new TaskHandler("typescript_example", "clear_task_state", clearTaskState),
  new TaskHandler("typescript_taskflow_example", "summarize", summarize),
  new TaskHandler("typescript_taskflow_example", "report", report),
  new TaskHandler("typescript_taskflow_example", "build_message", buildSummaryMessage),
);
await bundle.serve();
