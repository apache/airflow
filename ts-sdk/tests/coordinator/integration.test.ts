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

// Pure-Node integration test for the coordinator runtime.
//
// Stands up a minimal in-process "supervisor" (TCP server +
// length-prefixed msgpack frame codec) that mirrors what Airflow's
// real `BaseCoordinator._runtime_subprocess_entrypoint` does, then
// drives the runtime through task success, task failure, retry, abort signaling,
// task-time RPCs, missing handlers, and parse-mode responses.
//
// No Python, no Airflow install, but exercises the same wire format
// the real coordinator speaks.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createServer, Socket as NetSocket, type Server, type Socket } from "node:net";
import { encode, decode } from "@msgpack/msgpack";
import {
  COORDINATOR_RESPONSE_TIMEOUT_MS,
  startCoordinator,
} from "../../src/coordinator/runtime.js";
import { Dag } from "../../src/sdk/dag.js";
import { Bundle } from "../../src/sdk/bundle.js";
import { withArgNames } from "../../src/sdk/arg-names.js";
import { TaskHandler } from "../../src/sdk/task-handler.js";
import { getClient, getContext } from "../../src/sdk/task.js";

/** The arguments `py_dag.bound`'s Python call site passes, as its handler
 *  spells them. Folding absorbs the snake_case on the wire. */
interface BoundTransformArgs {
  regionCode: string;
  threshold: number;
  dryRun: boolean;
}

// The bundle the runtime dispatches through. startCoordinator() is driven
// directly rather than through bundle.serve(), so these tests can supply mock
// socket addresses. Both authoring kinds are registered in one call, since the
// runtime dispatches through one lookup regardless of which put a task there.
//
// Rebuilt per test: dispatching reads the bundle, which finalizes every native
// Dag in it against further tasks, so one Dag cannot collect a task per test.
let testDag: Dag;
let otherDag: Dag;
let bundle: Bundle;

beforeEach(() => {
  testDag = new Dag("test_dag");
  otherDag = new Dag("other_dag");
  bundle = new Bundle(testDag, otherDag);
});

interface MockResult {
  firstResponse: { id: number; body: unknown; isResponse: boolean } | null;
  logRecords: Record<string, unknown>[];
  runtimeRequests: { type: string; body: Record<string, unknown> }[];
}

/** Callback used by `driveSupervisor` to answer runtime-initiated
 *  requests. Return `{ body, error? }` to reply with that arity-3
 *  frame, or `null` to ignore the request (the runtime will hang
 *  waiting for a response, which is only useful for negative tests). */
type Responder = (
  msgType: string,
  body: Record<string, unknown>,
) => { body: unknown; error?: unknown } | null;

function frameBytes(id: number, body: unknown, isResponse: boolean): Buffer {
  const arr = isResponse ? [id, body, null] : [id, body];
  const payload = Buffer.from(encode(arr));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

function readFrames(buf: Buffer): {
  frames: { id: number; body: unknown; arity: number }[];
  rest: Buffer;
} {
  const out: { id: number; body: unknown; arity: number }[] = [];
  let rest: Buffer = buf;
  while (rest.length >= 4) {
    const len = rest.readUInt32BE(0);
    if (rest.length < 4 + len) break;
    const payload = rest.subarray(4, 4 + len);
    const arr = decode(Buffer.from(payload)) as unknown[];
    out.push({ id: arr[0] as number, body: arr[1], arity: arr.length });
    rest = Buffer.from(rest.subarray(4 + len));
  }
  return { frames: out, rest };
}

function makeStartupDetails(
  taskId: string,
  dagId = "test_dag",
  runId = "r1",
  tiContext: Record<string, unknown> = {},
): unknown {
  return {
    type: "StartupDetails",
    ti: {
      id: "ti-1",
      dag_version_id: "dag-version-1",
      task_id: taskId,
      dag_id: dagId,
      run_id: runId,
      try_number: 1,
      map_index: -1,
      hostname: "test-host",
      queue: "default",
    },
    dag_rel_path: "test.py",
    bundle_info: { name: "test", version: null },
    start_date: "2026-04-23T00:00:00Z",
    ti_context: tiContext,
    sentry_integration: "",
  };
}

async function listen(): Promise<{ server: Server; port: number }> {
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as { port: number }).port;
      resolve({ server, port });
    });
  });
}

async function acceptOne(server: Server): Promise<Socket> {
  return new Promise((resolve) => server.once("connection", resolve));
}

function xcomKey(body: Record<string, unknown>): string {
  return [body["dag_id"], body["run_id"], body["task_id"], body["key"]].join("|");
}

function isFrameWithBodyType(chunk: unknown, type: string): boolean {
  if (!Buffer.isBuffer(chunk)) return false;
  try {
    return readFrames(chunk).frames.some((f) => {
      const body = f.body as Record<string, unknown> | null;
      return body?.["type"] === type;
    });
  } catch {
    return false;
  }
}

async function driveSupervisor(initialFrame: unknown, responder?: Responder): Promise<MockResult> {
  const comm = await listen();
  const logs = await listen();

  const commAccept = acceptOne(comm.server);
  const logsAccept = acceptOne(logs.server);

  const runtimeDone = startCoordinator(bundle, {
    commAddr: `127.0.0.1:${comm.port}`,
    logsAddr: `127.0.0.1:${logs.port}`,
    argv: [],
  });

  const [commSock, logsSock] = await Promise.all([commAccept, logsAccept]);

  // Send the kickoff frame as a _ResponseFrame (arity 3), matching what
  // Airflow's `_send_startup_details` actually emits on the wire.
  commSock.write(frameBytes(0, initialFrame, true));

  let firstResponse: MockResult["firstResponse"] = null;
  const runtimeRequests: MockResult["runtimeRequests"] = [];
  const logChunks: Buffer[] = [];
  let commBuf: Buffer = Buffer.from(new Uint8Array(0));

  commSock.on("data", (chunk: Buffer) => {
    commBuf = Buffer.from(Buffer.concat([commBuf, chunk]));
    const taken = readFrames(commBuf);
    commBuf = taken.rest;
    for (const f of taken.frames) {
      if (f.arity === 2) {
        // Runtime-initiated request (mid-task RPC). Forward to the
        // responder; reply on the same id with an arity-3 frame.
        const body = (f.body ?? {}) as Record<string, unknown>;
        const msgType = String(body["type"] ?? "");
        runtimeRequests.push({ type: msgType, body });
        const reply = responder?.(msgType, body) ??
          // Default: ack any request with an empty body so the
          // runtime never hangs on auto-generated RPCs (e.g. the
          // return_value XCom push).
          { body: null };
        commSock.write(frameBytes(f.id, reply.body, true));
        continue;
      }
      // Arity-3: a response from the runtime. The terminal frame
      // (SucceedTask / TaskState) lands here.
      if (firstResponse === null) {
        firstResponse = { id: f.id, body: f.body, isResponse: true };
      }
    }
  });
  logsSock.on("data", (chunk: Buffer) => logChunks.push(chunk));

  // Wait for the runtime to finish AND for the comm socket to deliver
  // its final bytes (FIN signals that all preceding frames are flushed).
  const commSocketEnded = new Promise<void>((resolve) => commSock.on("end", () => resolve()));
  await Promise.all([runtimeDone, commSocketEnded]);

  comm.server.close();
  logs.server.close();

  const lines = Buffer.concat(logChunks).toString("utf8").split("\n").filter(Boolean);
  const logRecords = lines.map((l) => JSON.parse(l) as Record<string, unknown>);

  return { firstResponse, logRecords, runtimeRequests };
}

describe("coordinator runtime integration", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("closes the log socket when the comm connection fails during startup", async () => {
    const logs = await listen();
    const comm = await listen();

    const logsSockPromise = acceptOne(logs.server);
    const commSockPromise = acceptOne(comm.server);
    const runtimeDone = startCoordinator(bundle, {
      commAddr: `127.0.0.1:${comm.port}`,
      logsAddr: `127.0.0.1:${logs.port}`,
      argv: [],
    });
    const [logsSock, commSock] = await Promise.all([logsSockPromise, commSockPromise]);
    const logsSockEnded = new Promise<void>((resolve) => logsSock.on("end", () => resolve()));
    logsSock.resume();

    commSock.write(Buffer.from([0, 0, 0, 1, 0x2a]));
    await expect(runtimeDone).rejects.toThrow(/array frame/);
    await logsSockEnded;
    commSock.destroy();
    logs.server.close();
    comm.server.close();
  });

  it("dispatches StartupDetails to a registered handler and emits SucceedTask", async () => {
    let observedCtx: unknown = null;
    testDag.task("say_hello", async () => {
      observedCtx = getContext();
      return "ok";
    })();

    const result = await driveSupervisor(makeStartupDetails("say_hello"));

    expect(result.firstResponse).not.toBeNull();
    expect(result.firstResponse!.body).toMatchObject({
      type: "SucceedTask",
      task_outlets: [],
      outlet_events: [],
    });
    expect(observedCtx).toMatchObject({
      taskId: "say_hello",
      dagId: "test_dag",
      runId: "r1",
      tryNumber: 1,
      mapIndex: -1,
    });
    expect(result.logRecords.some((r) => r["event"] === "[ts-sdk.runtime] Task succeeded")).toBe(
      true,
    );

    // Logger names should be hierarchical (`ts-sdk.<subsystem>`) so the
    // Python supervisor's ConsoleRenderer prints them as a distinct
    // `[name]` column, not hardcoded to "task" (which collides with
    // user task logs).
    const loggers = new Set(result.logRecords.map((r) => r["logger"]));
    expect(loggers.has("ts-sdk.runtime")).toBe(true);
    for (const l of loggers) {
      expect(l).toMatch(/^ts-sdk(\.|$)/);
    }
  });

  it("times out when the terminal task response cannot be written", async () => {
    vi.useFakeTimers();
    const originalWrite = NetSocket.prototype.write;
    let stalledTerminalWrite: (() => void) | null = null;

    vi.spyOn(NetSocket.prototype, "write").mockImplementation(function (
      this: NetSocket,
      ...args: Parameters<NetSocket["write"]>
    ): boolean {
      if (isFrameWithBodyType(args[0], "SucceedTask")) {
        stalledTerminalWrite?.();
        return true;
      }
      return Reflect.apply(originalWrite, this, args) as boolean;
    });

    const comm = await listen();
    const logs = await listen();
    const commAccept = acceptOne(comm.server);
    const logsAccept = acceptOne(logs.server);

    testDag.task("terminal_timeout", async () => undefined)();
    const runtimeDone = startCoordinator(bundle, {
      commAddr: `127.0.0.1:${comm.port}`,
      logsAddr: `127.0.0.1:${logs.port}`,
      argv: [],
    });
    const assertion = expect(runtimeDone).rejects.toThrow(
      `Timed out sending response after ${COORDINATOR_RESPONSE_TIMEOUT_MS} ms`,
    );

    const [commSock, logsSock] = await Promise.all([commAccept, logsAccept]);
    logsSock.resume();
    try {
      const stalled = new Promise<void>((resolve) => {
        stalledTerminalWrite = resolve;
      });
      commSock.write(frameBytes(0, makeStartupDetails("terminal_timeout"), true));
      await stalled;
      await vi.advanceTimersByTimeAsync(COORDINATOR_RESPONSE_TIMEOUT_MS);

      await assertion;
    } finally {
      commSock.destroy();
      logsSock.destroy();
      comm.server.close();
      logs.server.close();
    }
  });

  it("returns TaskState=failed when the handler throws", async () => {
    testDag.task("boom", async () => {
      throw new Error("boom");
    })();

    const result = await driveSupervisor(makeStartupDetails("boom"));

    expect(result.firstResponse!.body).toMatchObject({
      type: "TaskState",
      state: "failed",
    });
  });

  it("returns RetryTask when the handler throws and Airflow says the failure is retryable", async () => {
    testDag.task("boom_retry", async () => {
      throw new Error("boom");
    })();

    const result = await driveSupervisor(
      makeStartupDetails("boom_retry", "test_dag", "r1", {
        should_retry: true,
        max_tries: 3,
      }),
    );

    expect(result.firstResponse!.body).toMatchObject({
      type: "RetryTask",
      retry_reason: "boom",
    });
  });

  it("dispatches to a task handler registered for a Python-owned Dag", async () => {
    // The mixed-language path: no Dag object exists for `py_dag` on this side,
    // only a handler bound to its dag_id and task_id.
    let observedCtx: unknown = null;
    bundle.register(
      new TaskHandler("py_dag", "transform", async () => {
        observedCtx = getContext();
        return "transformed";
      }),
    );

    const result = await driveSupervisor(makeStartupDetails("transform", "py_dag"));

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    expect(observedCtx).toMatchObject({ dagId: "py_dag", taskId: "transform" });
    const setXComReqs = result.runtimeRequests.filter((r) => r.type === "SetXCom");
    expect(setXComReqs[0]!.body).toMatchObject({
      key: "return_value",
      value: "transformed",
      dag_id: "py_dag",
      task_id: "transform",
    });
  });

  it("keeps two Dags' same-named tasks apart when dispatching", async () => {
    // Registration keys on the pair, and so must dispatch: the runtime is
    // handed dag_id and task_id together and must not answer from the wrong one.
    bundle.register(
      new TaskHandler("pair_dag_a", "shared", async () => "from a"),
      new TaskHandler("pair_dag_b", "shared", async () => "from b"),
    );

    for (const [dagId, expected] of [
      ["pair_dag_a", "from a"],
      ["pair_dag_b", "from b"],
    ]) {
      const result = await driveSupervisor(makeStartupDetails("shared", dagId));
      expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
      expect(result.runtimeRequests.find((r) => r.type === "SetXCom")!.body).toMatchObject({
        value: expected,
        dag_id: dagId,
      });
    }
  });

  it("hands a handler the arguments its Dag's call bound", async () => {
    // End of the folding path through the real wire format: the Python call
    // site's names arrive snake_case and the handler destructures camelCase.
    let observed: unknown = null;
    bundle.register(
      new TaskHandler(
        "py_dag",
        "bound",
        async ({ regionCode, threshold, dryRun }: BoundTransformArgs) => {
          observed = { regionCode, threshold, dryRun };
          return observed;
        },
      ),
    );

    const result = await driveSupervisor(
      makeStartupDetails("bound", "py_dag", "r1", {
        arg_bindings: [
          { name: "region_code", kind: "literal", value: "uk" },
          { name: "threshold", kind: "literal", value: 0.75 },
          { name: "dry_run", kind: "literal", value: false, from_default: true },
        ],
      }),
    );

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    expect(observed).toEqual({ regionCode: "uk", threshold: 0.75, dryRun: false });
  });

  it("resolves an upstream output its Dag's call passed, before the handler runs", async () => {
    // `summarize(make_totals(), "uk")` on the Python side: the runtime pulls
    // the upstream's return_value itself rather than the handler doing it.
    let observed: unknown = null;
    bundle.register(
      new TaskHandler(
        "py_dag",
        "upstream_bound",
        async ({ totals, regionCode }: { totals: { orders: number }; regionCode: string }) => {
          observed = { totals, regionCode };
          return observed;
        },
      ),
    );

    const responder: Responder = (msgType, body) => {
      if (msgType === "GetXCom") {
        return {
          body: { type: "XComResult", key: body["key"], value: { orders: 12 } },
        };
      }
      if (msgType === "SetXCom") return { body: null };
      return null;
    };

    const result = await driveSupervisor(
      makeStartupDetails("upstream_bound", "py_dag", "r1", {
        arg_bindings: [
          { name: "totals", kind: "xcom", task_id: "make_totals" },
          { name: "region_code", kind: "literal", value: "uk" },
        ],
      }),
      responder,
    );

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    expect(observed).toEqual({ totals: { orders: 12 }, regionCode: "uk" });
    // Pulled under the same key Python `@task` pushes a return value to, and
    // for the upstream the binding named rather than the running task.
    expect(result.runtimeRequests.find((r) => r.type === "GetXCom")!.body).toMatchObject({
      key: "return_value",
      task_id: "make_totals",
      dag_id: "py_dag",
    });
  });

  it("fails the task when an upstream it was called with pushed no output", async () => {
    // An unbound argument reaches the handler as `undefined` and corrupts its
    // output, so this stops the task instead, before it ran.
    let ran = false;
    bundle.register(
      new TaskHandler("py_dag", "missing_upstream", async () => {
        ran = true;
        return "should not run";
      }),
    );

    const responder: Responder = (msgType) =>
      msgType === "GetXCom" ? { body: { type: "ErrorResponse", error: "XCOM_NOT_FOUND" } } : null;

    const result = await driveSupervisor(
      makeStartupDetails("missing_upstream", "py_dag", "r1", {
        arg_bindings: [{ name: "totals", kind: "xcom", task_id: "make_totals" }],
      }),
      responder,
    );

    expect(ran).toBe(false);
    expect(result.firstResponse!.body).toMatchObject({ type: "TaskState", state: "failed" });
    expect(result.runtimeRequests.filter((r) => r.type === "SetXCom")).toHaveLength(0);
    expect(
      result.logRecords.some(
        (r) =>
          r["event"] === "[ts-sdk.runtime] Cannot bind this task's call arguments" &&
          String(r["error"]).includes("pushed no return_value XCom"),
      ),
    ).toBe(true);
  });

  it("honours a handler's explicit renames over folding", async () => {
    // `withArgNames` travels with the handler, so the dispatch site reads it
    // back off the function the bundle holds.
    let observed: unknown = null;
    bundle.register(
      new TaskHandler(
        "py_dag",
        "renamed",
        withArgNames(
          { label: "run_label" },
          async ({ label, regionCode }: { label: string; regionCode: string }) => {
            observed = { label, regionCode };
            return observed;
          },
        ),
      ),
    );

    const result = await driveSupervisor(
      makeStartupDetails("renamed", "py_dag", "r1", {
        arg_bindings: [
          { name: "run_label", kind: "literal", value: "nightly" },
          { name: "region_code", kind: "literal", value: "uk" },
        ],
      }),
    );

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    // `label` came from the map, `regionCode` folded as usual.
    expect(observed).toEqual({ label: "nightly", regionCode: "uk" });
  });

  it("fails the task when two of its bound names fold alike", async () => {
    // Reported before the handler runs, so nothing it might have written to
    // XCom is at stake.
    bundle.register(new TaskHandler("py_dag", "ambiguous", async () => "never runs"));

    const result = await driveSupervisor(
      makeStartupDetails("ambiguous", "py_dag", "r1", {
        arg_bindings: [
          { name: "region_code", kind: "literal", value: "uk" },
          { name: "regionCode", kind: "literal", value: "de" },
        ],
      }),
    );

    expect(result.firstResponse!.body).toMatchObject({ type: "TaskState", state: "failed" });
    expect(result.runtimeRequests.filter((r) => r.type === "SetXCom")).toHaveLength(0);
    expect(
      result.logRecords.some(
        (r) =>
          r["event"] === "[ts-sdk.runtime] Cannot bind this task's call arguments" &&
          String(r["error"]).includes('both fold to "regioncode"'),
      ),
    ).toBe(true);
  });

  it("names what a failing task's call bound", async () => {
    // A handler that destructured an argument under a name nothing folds to
    // gets no error of its own, so the failure report carries the names.
    bundle.register(
      new TaskHandler("py_dag", "misnamed", async () => {
        throw new Error("cannot proceed");
      }),
    );

    const result = await driveSupervisor(
      makeStartupDetails("misnamed", "py_dag", "r1", {
        arg_bindings: [{ name: "region_code", kind: "literal", value: "uk" }],
      }),
    );

    expect(result.firstResponse!.body).toMatchObject({ type: "TaskState", state: "failed" });
    expect(
      result.logRecords.some(
        (r) =>
          r["event"] === "[ts-sdk.runtime] Task failed" &&
          JSON.stringify(r["bound_args"]) === JSON.stringify(["region_code"]),
      ),
    ).toBe(true);
  });

  it("aborts the context signal on SIGTERM and reports a thrown task error", async () => {
    let sawAbort = false;
    testDag.task("aborted_then_failed", async () => {
      process.emit("SIGTERM");
      sawAbort = getContext().signal.aborted;
      throw new Error("interrupted");
    })();

    const result = await driveSupervisor(makeStartupDetails("aborted_then_failed"));

    expect(sawAbort).toBe(true);
    expect(result.firstResponse!.body).toMatchObject({
      type: "TaskState",
      state: "failed",
    });
    expect(result.runtimeRequests.filter((r) => r.type === "SetXCom")).toHaveLength(0);
  });

  it("returns RetryTask with the thrown error when a task fails after SIGTERM", async () => {
    let sawAbort = false;
    testDag.task("aborted_then_failed_retry", async () => {
      process.emit("SIGTERM");
      sawAbort = getContext().signal.aborted;
      throw new Error("interrupted");
    })();

    const result = await driveSupervisor(
      makeStartupDetails("aborted_then_failed_retry", "test_dag", "r1", {
        should_retry: true,
        max_tries: 3,
      }),
    );

    expect(sawAbort).toBe(true);
    expect(result.firstResponse!.body).toMatchObject({
      type: "RetryTask",
      retry_reason: "interrupted",
    });
    expect(result.runtimeRequests.filter((r) => r.type === "SetXCom")).toHaveLength(0);
  });

  it("does not discard a completed task result after SIGTERM", async () => {
    let sawAbort = false;
    testDag.task("completed_after_sigterm", async () => {
      process.emit("SIGTERM");
      sawAbort = getContext().signal.aborted;
      return "completed";
    })();

    const result = await driveSupervisor(makeStartupDetails("completed_after_sigterm"));

    expect(sawAbort).toBe(true);
    expect(result.firstResponse!.body).toMatchObject({
      type: "SucceedTask",
      task_outlets: [],
      outlet_events: [],
    });

    const setXComReqs = result.runtimeRequests.filter((r) => r.type === "SetXCom");
    expect(setXComReqs).toHaveLength(1);
    expect(setXComReqs[0]!.body).toMatchObject({
      key: "return_value",
      value: "completed",
    });
  });

  it("returns TaskState=removed when no handler is registered", async () => {
    const result = await driveSupervisor(makeStartupDetails("missing_task"));

    expect(result.firstResponse!.body).toMatchObject({
      type: "TaskState",
      state: "removed",
    });
  });

  it("exposes a client that round-trips GetVariable / SetXCom / GetXCom", async () => {
    const xcomStore = new Map<string, unknown>();
    let observedGreeting: string | null = "<unset>";

    testDag.task("say_hello_client", async () => {
      // The coordinator-mode handler MUST be able to reach a client.
      const ctx = getContext();
      const client = getClient();

      observedGreeting = await client.getVariable("e6_greeting");
      await client.setXCom({
        key: "echo",
        value: `node says: ${observedGreeting}`,
        dagId: ctx.dagId,
        taskId: ctx.taskId,
        runId: ctx.runId,
      });
      const back = await client.getXCom({
        key: "echo",
        dagId: ctx.dagId,
        taskId: ctx.taskId,
        runId: ctx.runId,
      });
      if (back !== `node says: ${observedGreeting}`) {
        throw new Error(`xcom round-trip mismatch: ${back}`);
      }
    })();

    const responder: Responder = (msgType, body) => {
      if (msgType === "GetVariable") {
        return {
          body: {
            type: "VariableResult",
            key: body["key"],
            value: "hello from airflow",
          },
        };
      }
      if (msgType === "SetXCom") {
        const k = xcomKey(body);
        xcomStore.set(k, body["value"]);
        // Supervisor sends an empty arity-3 frame on success.
        return { body: null };
      }
      if (msgType === "GetXCom") {
        const k = xcomKey(body);
        const value = xcomStore.get(k) ?? null;
        return {
          body: { type: "XComResult", key: body["key"], value },
        };
      }
      return null;
    };

    const result = await driveSupervisor(makeStartupDetails("say_hello_client"), responder);

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    expect(observedGreeting).toBe("hello from airflow");

    const requestTypes = result.runtimeRequests.map((r) => r.type);
    expect(requestTypes).toEqual(["GetVariable", "SetXCom", "GetXCom"]);

    const setReq = result.runtimeRequests.find((r) => r.type === "SetXCom")!.body;
    expect(setReq).toMatchObject({
      key: "echo",
      value: "node says: hello from airflow",
      dag_id: "test_dag",
      task_id: "say_hello_client",
      run_id: "r1",
    });
  });

  it("returns null from getVariable when the supervisor signals NOT_FOUND", async () => {
    let observed: string | null = "<unset>";
    testDag.task("missing_variable", async () => {
      observed = await getClient().getVariable("missing_key");
    })();

    const responder: Responder = (msgType) => {
      if (msgType === "GetVariable") {
        return {
          body: {
            type: "ErrorResponse",
            error: "VARIABLE_NOT_FOUND",
            detail: { key: "missing_key" },
          },
        };
      }
      return null;
    };

    const result = await driveSupervisor(makeStartupDetails("missing_variable"), responder);
    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });
    expect(observed).toBeNull();
  });

  it("looks up handlers by exact Dag and task id", async () => {
    let calledFirstDag = false;
    let calledSecondDag = false;
    testDag.task("shared_task", async () => {
      calledFirstDag = true;
    })();
    otherDag.task("shared_task", async () => {
      calledSecondDag = true;
    })();

    await driveSupervisor(makeStartupDetails("shared_task"));

    expect(calledFirstDag).toBe(true);
    expect(calledSecondDag).toBe(false);
  });

  it("returns empty serialized_dags for DagFileParseRequest", async () => {
    const parseRequest = {
      type: "DagFileParseRequest",
      file: "/dags/test.mjs",
      bundle_path: "/dags",
    };

    const result = await driveSupervisor(parseRequest);

    const body = result.firstResponse!.body as Record<string, unknown>;
    expect(body.type).toBe("DagFileParsingResult");
    expect(body.serialized_dags).toEqual([]);
  });

  it("auto-pushes return_value XCom when handler returns a value", async () => {
    testDag.task("pusher", async () => "my-result")();

    const responder: Responder = (msgType, _body) => {
      if (msgType === "SetXCom") return { body: null };
      return null;
    };

    const result = await driveSupervisor(makeStartupDetails("pusher"), responder);

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });

    const setXComReqs = result.runtimeRequests.filter((r) => r.type === "SetXCom");
    expect(setXComReqs).toHaveLength(1);
    expect(setXComReqs[0]!.body).toMatchObject({
      key: "return_value",
      value: "my-result",
    });
  });

  it("does NOT push return_value XCom when handler returns undefined", async () => {
    testDag.task("void_task", async () => {
      // no return value
    })();

    const result = await driveSupervisor(makeStartupDetails("void_task"));

    expect(result.firstResponse!.body).toMatchObject({ type: "SucceedTask" });

    const setXComReqs = result.runtimeRequests.filter((r) => r.type === "SetXCom");
    expect(setXComReqs).toHaveLength(0);
  });
});
