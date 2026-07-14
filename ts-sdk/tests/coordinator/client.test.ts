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

import { describe, it, expect } from "vitest";
import { createCoordinatorClient } from "../../src/coordinator/client.js";
import type { CommChannel } from "../../src/coordinator/comm-channel.js";
import type { TaskContext } from "../../src/sdk/task.js";

function fakeComm(frames: { body: unknown; error?: unknown }[]): CommChannel {
  let i = 0;
  return {
    request: async () => frames[i++],
  } as unknown as CommChannel;
}

const FAKE_CTX: TaskContext = {
  dagId: "d",
  taskId: "t",
  runId: "r",
  tryNumber: 1,
  mapIndex: -1,
  signal: new AbortController().signal,
};

function client(frames: { body: unknown; error?: unknown }[]) {
  return createCoordinatorClient(fakeComm(frames), FAKE_CTX);
}

describe("getVariable not-found contract", () => {
  it("returns null for the exact VARIABLE_NOT_FOUND code", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "VARIABLE_NOT_FOUND" } }]);
    expect(await c.getVariable("x")).toBeNull();
  });

  it("throws for a non-not-found ErrorResponse", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "API_SERVER_ERROR" } }]);
    await expect(c.getVariable("x")).rejects.toThrow(/API_SERVER_ERROR/);
  });

  it("does NOT treat a value that merely contains 'NOT_FOUND' as absence", async () => {
    const c = client([{ body: { type: "VariableResult", key: "x", value: "NOT_FOUND_LOL" } }]);
    expect(await c.getVariable("x")).toBe("NOT_FOUND_LOL");
  });

  it("does NOT treat an error code merely containing the substring as not-found", async () => {
    // "SOMETHING_NOT_FOUND_ISH" is not in the exact set → must throw.
    const c = client([{ body: { type: "ErrorResponse", error: "SOMETHING_NOT_FOUND_ISH" } }]);
    await expect(c.getVariable("x")).rejects.toThrow();
  });
});

describe("getVariableOrThrow", () => {
  it("returns the value when present", async () => {
    const c = client([{ body: { type: "VariableResult", key: "x", value: "v" } }]);
    expect(await c.getVariableOrThrow("x")).toBe("v");
  });

  it("throws VariableNotFoundError on missing key", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "VARIABLE_NOT_FOUND" } }]);
    await expect(c.getVariableOrThrow("x")).rejects.toThrow(/Variable not found: x/);
  });

  it("throws VariableNotFoundError on a null-valued result", async () => {
    const c = client([{ body: { type: "VariableResult", key: "x", value: null } }]);
    await expect(c.getVariableOrThrow("x")).rejects.toThrow(/Variable not found: x/);
  });
});

describe("getXCom not-found contract", () => {
  it("returns null for the exact XCOM_NOT_FOUND code", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "XCOM_NOT_FOUND" } }]);
    expect(await c.getXCom({ key: "k" })).toBeNull();
  });
});

describe("client is bound to TaskContext", () => {
  it("defaults dag/task/run + map_index from ctx; allows override", async () => {
    const sent: Record<string, unknown>[] = [];
    const recordingComm = {
      request: async (b: Record<string, unknown>) => {
        sent.push(b);
        return { body: null };
      },
    } as unknown as CommChannel;
    const c = createCoordinatorClient(recordingComm, FAKE_CTX);

    await c.setXCom({ key: "echo", value: 1 });
    await c.setXCom({
      key: "echo",
      value: 2,
      dagId: "other",
      taskId: "up",
      runId: "rX",
    });

    expect(sent[0]).toMatchObject({
      type: "SetXCom",
      key: "echo",
      value: 1,
      dag_id: "d",
      task_id: "t",
      run_id: "r",
      map_index: null,
    });
    expect(sent[1]).toMatchObject({
      dag_id: "other",
      task_id: "up",
      run_id: "rX",
    });
  });

  it("defaults getXCom locator fields from ctx with snake_case wire names", async () => {
    const sent: Record<string, unknown>[] = [];
    const recordingComm = {
      request: async (b: Record<string, unknown>) => {
        sent.push(b);
        return { body: { type: "XComResult", key: b.key, value: null } };
      },
    } as unknown as CommChannel;
    const c = createCoordinatorClient(recordingComm, FAKE_CTX);

    await c.getXCom({ key: "k" });

    expect(sent[0]).toEqual({
      type: "GetXCom",
      key: "k",
      dag_id: "d",
      task_id: "t",
      run_id: "r",
      map_index: null,
      include_prior_dates: false,
    });
  });

  it("maps camelCase public XCom options to snake_case supervisor fields", async () => {
    const sent: Record<string, unknown>[] = [];
    const recordingComm = {
      request: async (b: Record<string, unknown>) => {
        sent.push(b);
        // setXCom expects body=null; getXCom expects an XComResult.
        return b.type === "GetXCom"
          ? { body: { type: "XComResult", key: b.key, value: null } }
          : { body: null };
      },
    } as unknown as CommChannel;
    // ctx with a real map index — to prove -1 from opts wins over a
    // mapped ctx value (caller is explicitly asking "the non-mapped row").
    const mappedCtx: TaskContext = { ...FAKE_CTX, mapIndex: 3 };
    const c = createCoordinatorClient(recordingComm, mappedCtx);

    await c.setXCom({ key: "k", value: 1, mapIndex: -1 });
    await c.setXCom({ key: "k", value: 2, mapIndex: null });
    await c.getXCom({
      key: "k",
      dagId: "other_dag",
      taskId: "upstream",
      runId: "manual__1",
      mapIndex: -1,
      includePriorDates: true,
    });
    await c.setXCom({ key: "k", value: 3, mapIndex: 5 });

    expect(sent[0]).toMatchObject({ type: "SetXCom", map_index: null });
    expect(sent[1]).toMatchObject({ type: "SetXCom", map_index: null });
    expect(sent[2]).toMatchObject({
      type: "GetXCom",
      dag_id: "other_dag",
      task_id: "upstream",
      run_id: "manual__1",
      map_index: null,
      include_prior_dates: true,
    });
    expect(sent[3]).toMatchObject({ type: "SetXCom", map_index: 5 });
  });
});

describe("getConnection", () => {
  it("maps wire snake_case connection fields to public camelCase fields", async () => {
    const c = client([
      {
        body: {
          type: "ConnectionResult",
          conn_id: "warehouse",
          conn_type: "postgres",
          host: "db.local",
          schema: "analytics",
          login: "airflow",
          password: "secret",
          port: 5432,
          extra: "{}",
        },
      },
    ]);

    await expect(c.getConnection("warehouse")).resolves.toEqual({
      id: "warehouse",
      type: "postgres",
      host: "db.local",
      schema: "analytics",
      login: "airflow",
      password: "secret",
      port: 5432,
      extra: "{}",
    });
  });

  it("coerces absent optional wire fields to null public fields", async () => {
    const c = client([
      { body: { type: "ConnectionResult", conn_id: "bare", conn_type: "generic" } },
    ]);

    await expect(c.getConnection("bare")).resolves.toEqual({
      id: "bare",
      type: "generic",
      host: null,
      schema: null,
      login: null,
      password: null,
      port: null,
      extra: null,
    });
  });

  it("returns null for missing connections", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "CONNECTION_NOT_FOUND" } }]);
    expect(await c.getConnection("missing")).toBeNull();
  });
});
