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

import { isDeepStrictEqual } from "node:util";

import { Asset, Bundle, getClient, getContext, TaskHandler } from "apache-airflow-ts-sdk";

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

/** Declared as the `use_asset_state_store` inlet in `dags/typescript_example.py`. */
const ORDERS = new Asset({
  name: "typescript_example_orders",
  uri: "x-typescript-example://orders",
});

function expectRead(step: string, expected: unknown, actual: unknown) {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(
      `${step}: expected ${JSON.stringify(expected)}, read ${JSON.stringify(actual)}`,
    );
  }
}

/**
 * Calls every asset state store method on one asset, addressed by name and by URI. Each step
 * changes the state through one selector and checks it through the other, so the task fails
 * unless both reach the same asset. Asset state outlives the run, so it starts by clearing what
 * an earlier run left, and leaves only `summary` for the E2E test to read.
 */
export async function useAssetStateStore() {
  const stores = getClient().assetStateStore;
  const byName = stores.forAsset(ORDERS);
  const byNameRef = stores.forAsset(Asset.ref({ name: ORDERS.name }));
  const byUri = stores.forAsset(Asset.ref({ uri: ORDERS.uri }));

  await byUri.clear();

  await byName.set("scratch", "first");
  expectRead("set() by name", "first", await byUri.get("scratch"));
  await byUri.clear();
  expectRead("clear() by URI", null, await byName.get("scratch"));

  await byUri.set("deleted", "temporary");
  await byNameRef.delete("deleted");
  expectRead("delete() by name reference", null, await byUri.get("deleted"));

  const summary = { runId: getContext().runId, rows: [1, 2, 3] };
  await byNameRef.set("summary", summary);
  expectRead("set() by name reference", summary, await byUri.get("summary"));
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
  new TaskHandler("typescript_example", "use_asset_state_store", useAssetStateStore),
  new TaskHandler("typescript_taskflow_example", "summarize", summarize),
  new TaskHandler("typescript_taskflow_example", "report", report),
  new TaskHandler("typescript_taskflow_example", "build_message", buildSummaryMessage),
);
await bundle.serve();
