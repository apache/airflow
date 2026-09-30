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

import { afterEach, describe, it, expect, vi } from "vitest";
import { ConnectionNotFoundError } from "../../src/sdk/client.js";
import { NEVER_EXPIRE } from "../../src/sdk/client-types.js";
import { createCoordinatorClient } from "../../src/coordinator/client.js";
import type { CommChannel } from "../../src/coordinator/comm-channel.js";
import type { TaskClient } from "../../src/sdk/client.js";
import type { TaskContext } from "../../src/sdk/task.js";

const RETENTION_ENV_VAR = "AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS";

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

const FAKE_TI_ID = "01890f3e-0000-7000-8000-000000000001";

function client(frames: { body: unknown; error?: unknown }[]) {
  return createCoordinatorClient(fakeComm(frames), FAKE_CTX, FAKE_TI_ID);
}

/** Captures the request bodies sent, answering each one with `reply`. */
function recordingClient(reply: unknown = null) {
  const sent: Record<string, unknown>[] = [];
  const comm = {
    request: async (body: Record<string, unknown>) => {
      sent.push(body);
      return { body: reply };
    },
  } as unknown as CommChannel;
  return { client: createCoordinatorClient(comm, FAKE_CTX, FAKE_TI_ID), sent };
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

describe("setVariable", () => {
  it("sends the description the caller gave", async () => {
    const { client: c, sent } = recordingClient();

    await c.setVariable("threshold", "42", "rows above this take the slow path");

    expect(sent[0]).toEqual({
      type: "PutVariable",
      key: "threshold",
      value: "42",
      description: "rows above this take the slow path",
    });
  });

  it("sends description as null when the caller gives none", async () => {
    const { client: c, sent } = recordingClient();

    await c.setVariable("threshold", "42");

    expect(sent[0]).toEqual({
      type: "PutVariable",
      key: "threshold",
      value: "42",
      description: null,
    });
  });
});

describe("deleteVariable", () => {
  it("sends DeleteVariable and resolves on the supervisor's OKResponse", async () => {
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await expect(c.deleteVariable("threshold")).resolves.toBeUndefined();

    expect(sent[0]).toEqual({ type: "DeleteVariable", key: "threshold" });
  });
});

describe("writes do not read a supervisor 404 as absence", () => {
  it.each([
    ["setVariable", "PutVariable", (c: TaskClient) => c.setVariable("k", "v")],
    ["deleteVariable", "DeleteVariable", (c: TaskClient) => c.deleteVariable("k")],
    ["setXCom", "SetXCom", (c: TaskClient) => c.setXCom({ key: "k", value: 1 })],
    [
      "setTaskStateStore",
      "SetTaskStateStore",
      (c: TaskClient) => c.setTaskStateStore({ key: "k", value: 1 }),
    ],
    [
      "deleteTaskStateStore",
      "DeleteTaskStateStore",
      (c: TaskClient) => c.deleteTaskStateStore("k"),
    ],
    ["clearTaskStateStore", "ClearTaskStateStore", (c: TaskClient) => c.clearTaskStateStore()],
  ])("%s rejects", async (_name, op, call) => {
    const c = client([
      { body: null, error: { error: "API_SERVER_ERROR", detail: { status_code: 404 } } },
    ]);
    await expect(call(c)).rejects.toThrow(`${op} failed: API_SERVER_ERROR`);
  });
});

describe("getXCom not-found contract", () => {
  it("returns null for the exact XCOM_NOT_FOUND code", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "XCOM_NOT_FOUND" } }]);
    expect(await c.getXCom({ key: "k" })).toBeNull();
  });
});

describe("getXComEntry", () => {
  // `getXCom` answers null for an absent row and a stored null alike, which is
  // the friendlier shape for handler code but cannot drive a decision between
  // the two. Argument binding needs both: an upstream that pushed no output
  // fails the task, while one that pushed null binds null.
  it("reports an absent row as not found", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "XCOM_NOT_FOUND" } }]);
    expect(await c.getXComEntry({ key: "k" })).toEqual({ found: false, value: null });
  });

  it("reports a stored null as found", async () => {
    const c = client([{ body: { type: "XComResult", key: "k", value: null } }]);
    expect(await c.getXComEntry({ key: "k" })).toEqual({ found: true, value: null });
  });

  it("reports a stored value as found", async () => {
    const c = client([{ body: { type: "XComResult", key: "k", value: { orders: 12 } } }]);
    expect(await c.getXComEntry({ key: "k" })).toEqual({ found: true, value: { orders: 12 } });
  });

  it("is what getXCom reads, so both see one round-trip", async () => {
    const c = client([{ body: { type: "XComResult", key: "k", value: false } }]);
    // `false` also pins that getXCom's `?? null` does not flatten a falsy value.
    expect(await c.getXCom({ key: "k" })).toBe(false);
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
    const c = createCoordinatorClient(recordingComm, FAKE_CTX, FAKE_TI_ID);

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
    const c = createCoordinatorClient(recordingComm, FAKE_CTX, FAKE_TI_ID);

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
    // ctx with a real map index, to prove -1 from opts wins over a
    // mapped ctx value (caller is explicitly asking "the non-mapped row").
    const mappedCtx: TaskContext = { ...FAKE_CTX, mapIndex: 3 };
    const c = createCoordinatorClient(recordingComm, mappedCtx, FAKE_TI_ID);

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

describe("getConnectionOrThrow", () => {
  it("returns the connection when present", async () => {
    const c = client([
      { body: { type: "ConnectionResult", conn_id: "warehouse", conn_type: "postgres" } },
    ]);

    await expect(c.getConnectionOrThrow("warehouse")).resolves.toMatchObject({
      id: "warehouse",
      type: "postgres",
    });
  });

  it("throws ConnectionNotFoundError on a missing connection", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "CONNECTION_NOT_FOUND" } }]);
    const result = c.getConnectionOrThrow("missing");
    await expect(result).rejects.toThrow(ConnectionNotFoundError);
    await expect(result).rejects.toThrow(/Connection not found: missing/);
  });

  it("propagates non-not-found errors instead of ConnectionNotFoundError", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "API_SERVER_ERROR" } }]);
    await expect(c.getConnectionOrThrow("warehouse")).rejects.toThrow(/API_SERVER_ERROR/);
  });
});

describe("getTaskStateStore", () => {
  it("sends GetTaskStateStore with the bound ti_id and returns the stored value", async () => {
    const { client: c, sent } = recordingClient({ type: "TaskStateStoreResult", value: { n: 1 } });

    await expect(c.getTaskStateStore("k")).resolves.toEqual({ n: 1 });

    expect(sent[0]).toEqual({ type: "GetTaskStateStore", ti_id: FAKE_TI_ID, key: "k" });
  });

  it("returns null for the exact TASK_STORE_NOT_FOUND code", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "TASK_STORE_NOT_FOUND" } }]);
    expect(await c.getTaskStateStore("k")).toBeNull();
  });

  it("rejects for an API_SERVER_ERROR with status 404, which is not in its absence policy", async () => {
    const c = client([
      { body: null, error: { error: "API_SERVER_ERROR", detail: { status_code: 404 } } },
    ]);
    await expect(c.getTaskStateStore("k")).rejects.toThrow(/API_SERVER_ERROR/);
  });

  it("rejects for VARIABLE_NOT_FOUND, which is not in its absence policy", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "VARIABLE_NOT_FOUND" } }]);
    await expect(c.getTaskStateStore("k")).rejects.toThrow(/VARIABLE_NOT_FOUND/);
  });

  it("rejects on an empty key without sending a request", async () => {
    const { client: c, sent } = recordingClient();
    await expect(c.getTaskStateStore("")).rejects.toThrow(RangeError);
    expect(sent).toHaveLength(0);
  });
});

describe("getVariable does not read TASK_STORE_NOT_FOUND as absence", () => {
  it("rejects for TASK_STORE_NOT_FOUND, which is not in its absence policy", async () => {
    const c = client([{ body: { type: "ErrorResponse", error: "TASK_STORE_NOT_FOUND" } }]);
    await expect(c.getVariable("k")).rejects.toThrow(/TASK_STORE_NOT_FOUND/);
  });
});

describe("setTaskStateStore retention", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("sends expires_at null for NEVER_EXPIRE", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await c.setTaskStateStore({ key: "k", value: 1, retentionMs: NEVER_EXPIRE });

    expect(sent[0]).toEqual({
      type: "SetTaskStateStore",
      ti_id: FAKE_TI_ID,
      key: "k",
      value: 1,
      expires_at: null,
    });
  });

  it("sends now + retentionMs for a finite retentionMs", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await c.setTaskStateStore({ key: "k", value: 1, retentionMs: 60_000 });

    expect(sent[0]).toMatchObject({ expires_at: "2026-01-01T00:01:00.000Z" });
  });

  it("sends now (expire immediately) for retentionMs 0", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await c.setTaskStateStore({ key: "k", value: 1, retentionMs: 0 });

    expect(sent[0]).toMatchObject({ expires_at: "2026-01-01T00:00:00.000Z" });
  });

  it.each([
    ["absent", undefined, "2026-01-31T00:00:00.000Z"],
    ["", "", "2026-01-31T00:00:00.000Z"],
    ["0", "0", null],
    ["7", "7", "2026-01-08T00:00:00.000Z"],
    ["7.0", "7.0", "2026-01-08T00:00:00.000Z"],
  ])("resolves the default_retention_days env var: %s", async (_label, raw, expected) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
    vi.stubEnv(RETENTION_ENV_VAR, raw);
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await c.setTaskStateStore({ key: "k", value: 1 });

    expect(sent[0]).toMatchObject({ expires_at: expected });
  });

  it.each([
    ["-1", "-1"],
    ["abc", "abc"],
    ["fractional", "7.5"],
  ])("rejects an invalid default_retention_days env var: %s", async (_label, raw) => {
    vi.stubEnv(RETENTION_ENV_VAR, raw);
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await expect(c.setTaskStateStore({ key: "k", value: 1 })).rejects.toThrow(RangeError);
    expect(sent).toHaveLength(0);
  });
});

describe("setTaskStateStore guards", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    ["null", null],
    ["undefined", undefined],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["-Infinity", Number.NEGATIVE_INFINITY],
  ])("rejects a %s value without sending a request", async (_label, value) => {
    const { client: c, sent } = recordingClient();
    // @ts-expect-error exercising the runtime guard against an untyped caller.
    await expect(c.setTaskStateStore({ key: "k", value })).rejects.toThrow(TypeError);
    expect(sent).toHaveLength(0);
  });

  it.each([
    ["negative", -1],
    ["NaN", Number.NaN],
  ])("rejects retentionMs=%s without sending a request", async (_label, retentionMs) => {
    const { client: c, sent } = recordingClient();
    await expect(c.setTaskStateStore({ key: "k", value: 1, retentionMs })).rejects.toThrow(
      RangeError,
    );
    expect(sent).toHaveLength(0);
  });

  it("rejects an empty key without sending a request", async () => {
    const { client: c, sent } = recordingClient();
    await expect(c.setTaskStateStore({ key: "", value: 1 })).rejects.toThrow(RangeError);
    expect(sent).toHaveLength(0);
  });
});

describe("deleteTaskStateStore", () => {
  it("sends DeleteTaskStateStore and resolves on the supervisor's OKResponse", async () => {
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await expect(c.deleteTaskStateStore("k")).resolves.toBeUndefined();

    expect(sent[0]).toEqual({ type: "DeleteTaskStateStore", ti_id: FAKE_TI_ID, key: "k" });
  });

  it("rejects on an empty key without sending a request", async () => {
    const { client: c, sent } = recordingClient();
    await expect(c.deleteTaskStateStore("")).rejects.toThrow(RangeError);
    expect(sent).toHaveLength(0);
  });
});

describe("task state store key type guard", () => {
  it.each([
    [
      "getTaskStateStore",
      (c: TaskClient) => {
        // @ts-expect-error exercising the runtime guard against an untyped caller.
        return c.getTaskStateStore(undefined);
      },
    ],
    [
      "setTaskStateStore",
      (c: TaskClient) => {
        // @ts-expect-error exercising the runtime guard against an untyped caller.
        return c.setTaskStateStore({ key: undefined, value: 1 });
      },
    ],
    [
      "deleteTaskStateStore",
      (c: TaskClient) => {
        // @ts-expect-error exercising the runtime guard against an untyped caller.
        return c.deleteTaskStateStore(undefined);
      },
    ],
  ])("%s rejects a non-string key without sending a request", async (_label, call) => {
    const { client: c, sent } = recordingClient();
    await expect(call(c)).rejects.toThrow(TypeError);
    expect(sent).toHaveLength(0);
  });
});

describe("clearTaskStateStore", () => {
  it("sends ClearTaskStateStore with no key and resolves on the supervisor's OKResponse", async () => {
    const { client: c, sent } = recordingClient({ type: "OKResponse", ok: true });

    await expect(c.clearTaskStateStore()).resolves.toBeUndefined();

    expect(sent[0]).toEqual({ type: "ClearTaskStateStore", ti_id: FAKE_TI_ID });
  });
});
