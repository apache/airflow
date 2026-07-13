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
import type { TaskClient } from "../sdk/client.js";
import type { ConnectionResult, GetXComOpts, SetXComOpts } from "../sdk/client-types.js";
import { VariableNotFoundError } from "../sdk/client.js";
import type {
  GetVariable,
  GetXCom,
  SetXCom,
  GetConnection,
  ConnectionResult as WireConnectionResult,
} from "./protocol.js";

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

export function createCoordinatorClient(
  comm: CommChannel,
  ctx: TaskContext,
  logs: LogChannel | null = null,
): TaskClient {
  async function rpc<T>(
    op: string,
    expectedType: string | null,
    request: unknown,
    extract: (body: Record<string, unknown> | null) => T,
  ): Promise<T | null> {
    logs?.debug(`${op} request`);
    const frame = await comm.request(request);
    const err = parseFrameError(frame);
    if (err) {
      if (isNotFound(err)) {
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

  const client: TaskClient = {
    // ---- Variables ----

    async getVariable(key: string): Promise<string | null> {
      const msg: GetVariable = { type: "GetVariable", key };
      return rpc("GetVariable", "VariableResult", msg, (body) => (body!.value as string) ?? null);
    },

    async getVariableOrThrow(key: string): Promise<string> {
      const value = await client.getVariable(key);
      if (value == null) throw new VariableNotFoundError(key);
      return value;
    },

    // ---- XCom ----

    async getXCom<T = unknown>(opts: GetXComOpts): Promise<T | null> {
      const msg: GetXCom = {
        type: "GetXCom",
        key: opts.key,
        dag_id: opts.dagId ?? ctx.dagId,
        task_id: opts.taskId ?? ctx.taskId,
        run_id: opts.runId ?? ctx.runId,
        map_index: resolveWireMapIndex(opts.mapIndex, ctx.mapIndex),
        include_prior_dates: opts.includePriorDates ?? false,
      };
      return rpc("GetXCom", "XComResult", msg, (body) => (body!.value as T) ?? null);
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
      await rpc("SetXCom", null, msg, () => undefined);
    },

    // ---- Connections ----

    async getConnection(connId: string): Promise<ConnectionResult | null> {
      const msg: GetConnection = { type: "GetConnection", conn_id: connId };
      return rpc("GetConnection", "ConnectionResult", msg, (body) =>
        fromWireConnection(body as unknown as WireConnectionResult),
      );
    },
  };
  return client;
}

// -------- Error handling (two functions) --------
//
// parseFrameError: extract a structured error from the frame (once).
// isNotFound: decide if the error means "absent" (return null to caller)
//             or "failed" (throw).

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

// Exact supervisor ErrorType codes that mean "absent", not "failed".
const NOT_FOUND_CODES = new Set(["VARIABLE_NOT_FOUND", "XCOM_NOT_FOUND", "CONNECTION_NOT_FOUND"]);

/** Is this error a "not found" (caller should get null, not a throw)?
 *  Covers both dedicated NOT_FOUND codes and API_SERVER_ERROR with 404. */
function isNotFound(err: FrameError): boolean {
  if (NOT_FOUND_CODES.has(err.code)) return true;
  // The supervisor wraps API server 404s as API_SERVER_ERROR with
  // detail.status_code=404 (supervisor.py: WatchedSubprocess.handle_requests).
  // Dag / Dag run lookups hit this path.
  // TODO: If the TS client adds APIs beyond variables, XCom, and connections,
  // make not-found handling operation-specific instead of treating every
  // API_SERVER_ERROR 404 as null.
  return err.code === "API_SERVER_ERROR" && err.statusCode === 404;
}
