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

import type { CommChannel } from "./comm-channel.js";
import type { LogChannel } from "./log-channel.js";
import type { TaskContext } from "../sdk/task.js";
import type { TaskClient, TaskStateStore } from "../sdk/client.js";
import type {
  ConnectionResult,
  GetXComOpts,
  JsonValue,
  SetXComOpts,
  TaskStateStoreSetOpts,
} from "../sdk/client-types.js";
import { NEVER_EXPIRE } from "../sdk/client-types.js";
import { ConnectionNotFoundError, VariableNotFoundError } from "../sdk/client.js";
import type {
  GetVariable,
  PutVariable,
  DeleteVariable,
  GetXCom,
  SetXCom,
  GetConnection,
  SkipDownstreamTasks,
  TriggerDagRun,
  GetDagRunState,
  GetDag,
  ClearTaskStateStore,
  DeleteTaskStateStore,
  GetTaskStateStore,
  SetTaskStateStore,
  ConnectionResult as WireConnectionResult,
} from "./protocol.js";

/**
 * What a supervisor "row is absent" error means for an operation.
 *
 * `"throw"` always throws (a swallowed error on a `void` call would read as
 * success); otherwise, the `ErrorType` codes that mean "absent" plus whether
 * a wrapped API server 404 also counts (see `isAbsent`).
 */
type AbsentRowPolicy = "throw" | { codes: readonly string[]; apiServer404: boolean };

function resolveWireMapIndex(
  requestedMapIndex: number | null | undefined,
  contextMapIndex: number,
): number | null {
  // `mapIndex` is nullable, so only use the context value when the user did
  // not provide one. If the user passes null, send null to the supervisor.
  const mapIndex = requestedMapIndex === undefined ? contextMapIndex : requestedMapIndex;
  return mapIndex == null || mapIndex < 0 ? null : mapIndex;
}

function fromWireConnection(body: WireConnectionResult): ConnectionResult {
  return {
    id: body.conn_id,
    type: body.conn_type,
    host: body.host ?? null,
    schema: body.schema ?? null,
    login: body.login ?? null,
    password: body.password ?? null,
    port: body.port ?? null,
    extra: body.extra ?? null,
  };
}

/** The outcome of an XCom pull, keeping an absent row distinct from a stored null. */
export interface XComEntry {
  readonly found: boolean;
  /** `null` both for a stored null and for an absent row. */
  readonly value: JsonValue;
}

const XCOM_ABSENT: XComEntry = { found: false, value: null };

/**
 * A task's {@link TaskClient} plus the reads only the runtime itself makes.
 * Handlers are typed against `TaskClient`, so nothing added here is public API.
 */
export interface CoordinatorClient extends TaskClient {
  /**
   * Pull an XCom, reporting whether the row exists.
   *
   * {@link TaskClient.getXCom} answers `null` for an absent row and a stored
   * null alike. Argument binding needs them apart: an upstream that pushed no
   * output fails the task, while one that pushed null binds null.
   */
  getXComEntry(opts: GetXComOpts): Promise<XComEntry>;

  /** Mark direct downstream tasks of the running task as skipped; none is a no-op. */
  skipDownstreamTasks(taskIds: readonly string[]): Promise<void>;

  /**
   * Trigger a Dag run. `"already_exists"` is the one refusal a trigger task
   * handles itself; any other error throws.
   */
  triggerDagRun(msg: Omit<TriggerDagRun, "type">): Promise<"triggered" | "already_exists">;
  /** The state of a Dag run, as `GetDagRunState` reports it. */
  getDagRunState(dagId: string, runId: string): Promise<string>;
  /** Whether a Dag is paused. */
  isDagPaused(dagId: string): Promise<boolean>;
}

const VARIABLE_ABSENT_POLICY: AbsentRowPolicy = {
  codes: ["VARIABLE_NOT_FOUND"],
  apiServer404: true,
};
const XCOM_ABSENT_POLICY: AbsentRowPolicy = { codes: ["XCOM_NOT_FOUND"], apiServer404: true };
const CONNECTION_ABSENT_POLICY: AbsentRowPolicy = {
  codes: ["CONNECTION_NOT_FOUND"],
  apiServer404: true,
};
const TASK_STATE_STORE_ABSENT_POLICY: AbsentRowPolicy = {
  codes: ["TASK_STORE_NOT_FOUND"],
  apiServer404: false,
};

// A language SDK runtime cannot read Airflow config, so the coordinator passes
// `[state_store] default_retention_days` at launch
// (task-sdk/src/airflow/sdk/coordinators/_subprocess.py).
const RETENTION_DAYS_ENV_VAR = "AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS";
const MS_PER_DAY = 24 * 60 * 60 * 1000;

function assertKey(key: string): void {
  if (typeof key !== "string") {
    throw new TypeError(`task state store key must be a string, got ${typeof key}`);
  }
  if (key === "") {
    throw new RangeError("task state store key must not be empty");
  }
}

// Python's `datetime` (and the wire format it parses) cannot represent a year
// past 9999, and the supervisor silently drops a frame it cannot decode
// rather than replying with an error, so the task would hang instead of
// failing fast. Route both retention paths through this check.
function toExpiresAt(ms: number): string {
  const at = new Date(Date.now() + ms);
  if (Number.isNaN(at.getTime()) || at.getUTCFullYear() > 9999) {
    throw new RangeError(
      `retention of ${ms}ms overflows the wire timestamp; use NEVER_EXPIRE instead`,
    );
  }
  return at.toISOString();
}

/** Resolve `retentionMs` (or the deployment default) to a wire `expires_at`. */
function resolveExpiresAt(retentionMs: number | typeof NEVER_EXPIRE | undefined): string | null {
  if (retentionMs === undefined) {
    return resolveDefaultExpiresAt();
  }
  if (retentionMs === NEVER_EXPIRE) {
    return null;
  }
  if (!Number.isFinite(retentionMs) || retentionMs < 0) {
    throw new RangeError(`retentionMs must be >= 0 or NEVER_EXPIRE, got ${String(retentionMs)}`);
  }
  return toExpiresAt(retentionMs);
}

// The coordinator always passes the setting, so an absent or malformed value is
// a misconfiguration and fails the write rather than silently retaining the key
// for a period nobody configured.
function resolveDefaultExpiresAt(): string | null {
  const raw = process.env[RETENTION_DAYS_ENV_VAR];
  if (raw === undefined) {
    throw new Error(
      `${RETENTION_DAYS_ENV_VAR} is not set, so the default retention is unknown. It carries ` +
        "[state_store] default_retention_days to the runtime; pass retentionMs or NEVER_EXPIRE to " +
        "set the expiry explicitly.",
    );
  }
  const days = Number(raw);
  if (raw.trim() === "" || !Number.isInteger(days)) {
    throw new RangeError(`[state_store] default_retention_days must be a whole number, got ${raw}`);
  }
  if (days < 0) {
    throw new RangeError(
      `[state_store] default_retention_days must be >= 0, got ${raw}. Set to 0 to disable expiry.`,
    );
  }
  return days === 0 ? null : toExpiresAt(days * MS_PER_DAY);
}

export function createCoordinatorClient(
  comm: CommChannel,
  ctx: TaskContext,
  tiId: string,
  logs: LogChannel | null = null,
): CoordinatorClient {
  async function rpc<T>(
    op: string,
    expectedType: string | null,
    request: unknown,
    extract: (body: Record<string, unknown> | null) => T,
    absent: AbsentRowPolicy,
    allowedError?: string,
  ): Promise<T | null> {
    logs?.debug(`${op} request`);
    const frame = await comm.request(request);
    const err = parseFrameError(frame);
    if (err) {
      if (err.code === allowedError) {
        logs?.debug(`${op} answered ${err.code}`);
        return null;
      }
      if (absent !== "throw" && isAbsent(err, absent)) {
        logs?.debug(`${op} not found`, { error: err.code });
        return null;
      }
      logs?.warning(`${op} failed`, { error: err.code });
      throw new Error(`${op} failed: ${err.code}`);
    }
    const body = frame.body as Record<string, unknown> | null;
    if (expectedType !== null && body?.type !== expectedType) {
      logs?.error(`${op} unexpected response type`, {
        expected: expectedType,
        got: body?.type ?? null,
      });
      throw new Error(`${op}: unexpected response type ${JSON.stringify(body?.type)}`);
    }
    logs?.debug(`${op} ok`);
    return extract(body);
  }

  const taskStateStore: TaskStateStore = {
    async get<T = unknown>(key: string): Promise<T | null> {
      assertKey(key);
      const msg: GetTaskStateStore = { type: "GetTaskStateStore", ti_id: tiId, key };
      return rpc(
        "GetTaskStateStore",
        "TaskStateStoreResult",
        msg,
        (body) => body!.value as T,
        TASK_STATE_STORE_ABSENT_POLICY,
      );
    },

    async set(
      key: string,
      value: NonNullable<JsonValue>,
      opts: TaskStateStoreSetOpts = {},
    ): Promise<void> {
      assertKey(key);
      if (
        value === null ||
        value === undefined ||
        (typeof value === "number" && !Number.isFinite(value))
      ) {
        throw new TypeError("task state store value must not be null, NaN, or Infinity");
      }
      // TODO: warn when the serialized value exceeds the deployment's
      // [state_store] max_value_storage_bytes, as Python's task store setter
      // does, once the coordinator passes that setting to the runtime.
      const msg: SetTaskStateStore = {
        type: "SetTaskStateStore",
        ti_id: tiId,
        key,
        value,
        expires_at: resolveExpiresAt(opts.retentionMs),
      };
      await rpc("SetTaskStateStore", "OKResponse", msg, () => undefined, "throw");
    },

    async delete(key: string): Promise<void> {
      assertKey(key);
      const msg: DeleteTaskStateStore = { type: "DeleteTaskStateStore", ti_id: tiId, key };
      await rpc("DeleteTaskStateStore", "OKResponse", msg, () => undefined, "throw");
    },

    async clear(): Promise<void> {
      const msg: ClearTaskStateStore = { type: "ClearTaskStateStore", ti_id: tiId };
      await rpc("ClearTaskStateStore", "OKResponse", msg, () => undefined, "throw");
    },
  };

  const client: CoordinatorClient = {
    // ---- Variables ----

    async getVariable(key: string): Promise<string | null> {
      const msg: GetVariable = { type: "GetVariable", key };
      return rpc(
        "GetVariable",
        "VariableResult",
        msg,
        (body) => (body!.value as string) ?? null,
        VARIABLE_ABSENT_POLICY,
      );
    },

    async getVariableOrThrow(key: string): Promise<string> {
      const value = await client.getVariable(key);
      if (value == null) throw new VariableNotFoundError(key);
      return value;
    },

    async setVariable(key: string, value: string, description?: string | null): Promise<void> {
      // `description` is a required wire field the supervisor validates, so it
      // is always sent; null is what Python's `Variable.set` stores by default.
      const msg: PutVariable = {
        type: "PutVariable",
        key,
        value,
        description: description ?? null,
      };
      await rpc("PutVariable", null, msg, () => undefined, "throw");
    },

    async deleteVariable(key: string): Promise<void> {
      const msg: DeleteVariable = { type: "DeleteVariable", key };
      await rpc("DeleteVariable", "OKResponse", msg, () => undefined, "throw");
    },

    // ---- XCom ----

    async getXComEntry(opts: GetXComOpts): Promise<XComEntry> {
      const msg: GetXCom = {
        type: "GetXCom",
        key: opts.key,
        dag_id: opts.dagId ?? ctx.dagId,
        task_id: opts.taskId ?? ctx.taskId,
        run_id: opts.runId ?? ctx.runId,
        map_index: resolveWireMapIndex(opts.mapIndex, ctx.mapIndex),
        include_prior_dates: opts.includePriorDates ?? false,
      };
      const entry = await rpc<XComEntry>(
        "GetXCom",
        "XComResult",
        msg,
        (body) => ({
          found: true,
          // A row storing null arrives as an XComResult carrying null, so the
          // result frame decides `found` rather than the value.
          value: (body!.value ?? null) as JsonValue,
        }),
        XCOM_ABSENT_POLICY,
      );
      // `rpc` answers null for the supervisor's XCOM_NOT_FOUND.
      return entry ?? XCOM_ABSENT;
    },

    async getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null> {
      const { value } = await client.getXComEntry(opts);
      return (value as unknown as T) ?? null;
    },

    async setXCom(opts: SetXComOpts): Promise<void> {
      const msg: SetXCom = {
        type: "SetXCom",
        key: opts.key,
        value: opts.value,
        dag_id: opts.dagId ?? ctx.dagId,
        task_id: opts.taskId ?? ctx.taskId,
        run_id: opts.runId ?? ctx.runId,
        map_index: resolveWireMapIndex(opts.mapIndex, ctx.mapIndex),
      };
      await rpc("SetXCom", null, msg, () => undefined, "throw");
    },

    // ---- Control flow ----

    async skipDownstreamTasks(taskIds: readonly string[]): Promise<void> {
      if (taskIds.length === 0) return;
      const msg: SkipDownstreamTasks = { type: "SkipDownstreamTasks", tasks: [...taskIds] };
      await rpc("SkipDownstreamTasks", null, msg, () => undefined, "throw");
    },

    // ---- Dag runs ----

    async triggerDagRun(msg: Omit<TriggerDagRun, "type">) {
      const request: TriggerDagRun = { type: "TriggerDagRun", ...msg };
      const triggered = await rpc(
        "TriggerDagRun",
        null,
        request,
        () => "triggered" as const,
        "throw",
        "DAGRUN_ALREADY_EXISTS",
      );
      return triggered ?? "already_exists";
    },

    async getDagRunState(dagId: string, runId: string): Promise<string> {
      const msg: GetDagRunState = { type: "GetDagRunState", dag_id: dagId, run_id: runId };
      const state = await rpc(
        "GetDagRunState",
        "DagRunStateResult",
        msg,
        (body) => body!.state as string,
        "throw",
      );
      return state!;
    },

    async isDagPaused(dagId: string): Promise<boolean> {
      const msg: GetDag = { type: "GetDag", dag_id: dagId };
      const paused = await rpc(
        "GetDag",
        "DagResult",
        msg,
        (body) => body!.is_paused === true,
        "throw",
      );
      return paused!;
    },

    // ---- Task state store ----

    taskStateStore,

    // ---- Connections ----

    async getConnection(connId: string): Promise<ConnectionResult | null> {
      const msg: GetConnection = { type: "GetConnection", conn_id: connId };
      return rpc(
        "GetConnection",
        "ConnectionResult",
        msg,
        (body) => fromWireConnection(body as unknown as WireConnectionResult),
        CONNECTION_ABSENT_POLICY,
      );
    },

    async getConnectionOrThrow(connId: string): Promise<ConnectionResult> {
      const connection = await client.getConnection(connId);
      if (connection == null) throw new ConnectionNotFoundError(connId);
      return connection;
    },
  };
  return client;
}

// -------- Error handling (two functions) --------
//
// parseFrameError: extract a structured error from the frame (once).
// isAbsent: decide, per operation policy, if the error means "absent" (a
//           lookup returns null) or "failed" (throw).

interface FrameError {
  code: string;
  statusCode?: number;
}

/** Extract the error code and optional HTTP status from a response frame.
 *  Returns `null` for non-error frames. */
function parseFrameError(frame: { body: unknown; error?: unknown }): FrameError | null {
  // Case 1: error field on the frame itself
  if (frame.error != null) {
    if (typeof frame.error === "string") return { code: frame.error };
    if (typeof frame.error === "object") {
      const e = frame.error as Record<string, unknown>;
      if (typeof e.error === "string") {
        const detail = e.detail as { status_code?: number } | undefined;
        return { code: e.error, statusCode: detail?.status_code };
      }
    }
  }
  // Case 2: ErrorResponse in body
  const body = frame.body as Record<string, unknown> | null;
  if (body?.type === "ErrorResponse" && typeof body.error === "string") {
    const detail = body.detail as { status_code?: number } | undefined;
    return { code: body.error, statusCode: detail?.status_code };
  }
  return null;
}

/** Is this error "absent" for `policy` (caller should get null, not a throw)?
 *  The supervisor wraps API server 404s as API_SERVER_ERROR with
 *  detail.status_code=404 (supervisor.py: WatchedSubprocess.handle_requests). */
function isAbsent(
  err: FrameError,
  policy: { codes: readonly string[]; apiServer404: boolean },
): boolean {
  if (policy.codes.includes(err.code)) return true;
  return policy.apiServer404 && err.code === "API_SERVER_ERROR" && err.statusCode === 404;
}
