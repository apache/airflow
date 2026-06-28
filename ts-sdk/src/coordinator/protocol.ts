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

// Coordinator message types over the generated supervisor wire schema.
//
// The generated schema is authoritative for field names and payload shapes,
// but a few union discriminators are optional because they originate as
// defaulted Pydantic literals. This wrapper makes those discriminators
// required where TypeScript needs narrowing, and tightens the small number of
// runtime messages this SDK sends.

import type {
  StartupDetails as RawStartupDetails,
  DagFileParseRequest as RawDagFileParseRequest,
  ErrorResponse as RawErrorResponse,
  RetryTask as RawRetryTask,
  SucceedTask as RawSucceedTask,
  TaskState as RawTaskState,
  DagFileParsingResult as RawDagFileParsingResult,
} from "../generated/supervisor.js";

export { SUPERVISOR_API_VERSION } from "../generated/supervisor.js";

// -------- Re-exports — generated atoms used by client / runtime --------
//
// These are clean enough out of the box: small, no discriminator
// narrowing issues for callers (we treat them as request/response
// payloads, not as union members).
export type {
  VariableResult,
  XComResult,
  ConnectionResult,
  GetVariable,
  GetXCom,
  SetXCom,
  GetConnection,
} from "../generated/supervisor.js";

// -------- Frames from supervisor (narrowed discriminators) --------

export type StartupDetails = Omit<RawStartupDetails, "type"> & {
  type: "StartupDetails";
};

export type DagFileParseRequest = Omit<RawDagFileParseRequest, "type"> & {
  type: "DagFileParseRequest";
};

export type ErrorResponse = Omit<RawErrorResponse, "type"> & {
  type: "ErrorResponse";
};

export type MsgFromSupervisor = StartupDetails | DagFileParseRequest | ErrorResponse;

// -------- Frames from runtime (narrowed discriminators) --------

/** SucceedTask — schema marks task_outlets / outlet_events as optional,
 *  but the supervisor's Execution API rejects null for these fields, so
 *  we narrow both to required (empty array when none). */
export type SucceedTask = Omit<RawSucceedTask, "type" | "task_outlets" | "outlet_events"> & {
  type: "SucceedTask";
  task_outlets: unknown[];
  outlet_events: unknown[];
};

/** TaskState — schema types `state` as a bare string; narrow to the
 *  values the SDK actually sends so callers get an autocomplete-friendly
 *  union and typos fail at compile time. */
export type TaskStateMsg = Omit<RawTaskState, "type" | "state"> & {
  type: "TaskState";
  state: "failed" | "skipped" | "removed";
};

export type RetryTask = Omit<RawRetryTask, "type"> & {
  type: "RetryTask";
};

export type DagFileParsingResult = Omit<RawDagFileParsingResult, "type"> & {
  type: "DagFileParsingResult";
};

export type MsgFromRuntime = SucceedTask | TaskStateMsg | RetryTask | DagFileParsingResult;

// -------- Decoder: raw map → typed message --------

export function asMsgFromSupervisor(raw: unknown): MsgFromSupervisor {
  const body = normalizeBody(raw);
  switch (body.type) {
    case "StartupDetails":
    case "DagFileParseRequest":
    case "ErrorResponse":
      return body as unknown as MsgFromSupervisor;
    default:
      throw new Error(`Unsupported supervisor message type: ${JSON.stringify(body.type)}`);
  }
}

function normalizeBody(raw: unknown): { type: string; [k: string]: unknown } {
  if (raw === null || typeof raw !== "object") {
    throw new Error(`Frame body must be a map, got ${typeof raw}`);
  }
  const mapLike = raw as Record<string, unknown>;
  const type = mapLike["type"];
  if (typeof type !== "string") {
    throw new Error(
      `Frame body missing string 'type'; got keys: ${Object.keys(mapLike).join(",")}`,
    );
  }
  return { ...mapLike, type };
}
