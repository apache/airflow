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
// The generated schema is authoritative for field names and payload shapes.
// This module keeps the runtime decoder next to the generated type exports
// used by the coordinator client.

import type {
  StartupDetails,
  DagFileParseRequest,
  ErrorResponse,
  DagFileParsingResult,
  RetryTask,
  SucceedTask,
  TaskState,
} from "../generated/supervisor.js";

export { SUPERVISOR_API_VERSION } from "../generated/supervisor.js";

// -------- Re-exports — supervisor frames --------

export type {
  StartupDetails,
  DagFileParseRequest,
  ErrorResponse,
} from "../generated/supervisor.js";

// -------- Re-exports — runtime responses --------

export type {
  DagFileParsingResult,
  RetryTask,
  SucceedTask,
  TaskState,
} from "../generated/supervisor.js";

// -------- Re-exports — client RPC payloads --------

export type {
  VariableResult,
  XComResult,
  ConnectionResult,
  GetVariable,
  GetXCom,
  SetXCom,
  GetConnection,
} from "../generated/supervisor.js";

// -------- Frames from supervisor --------

type WithRequiredType<T extends { type?: unknown }> = Omit<T, "type"> & {
  type: NonNullable<T["type"]>;
};

export type MsgFromSupervisor =
  | WithRequiredType<StartupDetails>
  | WithRequiredType<DagFileParseRequest>
  | WithRequiredType<ErrorResponse>;

// -------- Frames to supervisor --------

export type RuntimeTaskState = WithRequiredType<TaskState>;
export type RuntimeRetryTask = WithRequiredType<RetryTask>;
export type RuntimeSucceedTask = WithRequiredType<SucceedTask> & {
  task_outlets: NonNullable<SucceedTask["task_outlets"]>;
  outlet_events: NonNullable<SucceedTask["outlet_events"]>;
};
export type RuntimeDagFileParsingResult = WithRequiredType<DagFileParsingResult>;

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
