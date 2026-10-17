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

import type { JsonValue } from "./client-types.js";
import { isPlainRecord } from "./dag.js";
import { getBooleanEnv } from "./env.js";
import { brandOperator, type Operator } from "./operator.js";
import { toPlainJson } from "./plain-json.js";
import { executeTriggerDagRun, resumeTriggerDagRun } from "./trigger-dag-run-execute.js";

/** A Dag run state, as `allowedStates` and `failedStates` name one. */
export type DagRunState = "queued" | "running" | "success" | "failed";

const DAG_RUN_STATES: ReadonlySet<string> = new Set<DagRunState>([
  "queued",
  "running",
  "success",
  "failed",
]);

/** The `TriggerDagRunOperator` options, named as TypeScript spells them. */
export interface TriggerDagRunSpec {
  /** Identifier of the Dag to trigger. */
  readonly dagId: string;
  /** Run ID for the triggered run; generated from runAfter or logicalDate when unset. */
  readonly runId?: string;
  /** Logical date of the triggered run; null creates a run without a logical date. */
  readonly logicalDate?: Date | null;
  /** Earliest time at which the triggered run may start. */
  readonly runAfter?: Date;
  /** Configuration the triggered run is started with. */
  readonly conf?: Readonly<Record<string, JsonValue>>;
  /** Clear an existing run with the same ID instead of failing. */
  readonly resetDagRun?: boolean;
  /** Hold this task open until the triggered run finishes. */
  readonly waitForCompletion?: boolean;
  /** Seconds between checks while waiting. Defaults to 60. */
  readonly pokeInterval?: number;
  /** Run states that count as success when waiting. Defaults to `["success"]`. */
  readonly allowedStates?: readonly DagRunState[];
  /** Run states that count as failure when waiting. Defaults to `["failed"]`. */
  readonly failedStates?: readonly DagRunState[];
  /** Skip rather than fail when the run already exists. */
  readonly skipWhenAlreadyExists?: boolean;
  /** Fail rather than trigger when the target Dag is paused. */
  readonly failWhenDagIsPaused?: boolean;
  /** Note recorded against the triggered run. */
  readonly note?: string;
  /**
   * While waiting, free the worker slot and defer to `DagStateTrigger`, which
   * the Python triggerer runs. Defaults to `[operators] default_deferrable`, or
   * to false when that is unset.
   */
  readonly deferrable?: boolean;
}

/** Internal: a trigger task's options with `TriggerDagRunOperator`'s defaults applied. */
export interface TriggerDagRunTask extends Operator<void, void> {
  readonly dagId: string;
  readonly runId: string | undefined;
  readonly logicalDate: Date | null | undefined;
  readonly runAfter: Date | undefined;
  readonly conf: Readonly<Record<string, JsonValue>> | undefined;
  readonly resetDagRun: boolean;
  readonly waitForCompletion: boolean;
  readonly pokeInterval: number;
  readonly allowedStates: readonly DagRunState[];
  readonly failedStates: readonly DagRunState[];
  readonly skipWhenAlreadyExists: boolean;
  readonly failWhenDagIsPaused: boolean;
  readonly note: string | undefined;
  readonly deferrable: boolean;
}

const OPTION_NAMES: ReadonlySet<string> = new Set<keyof TriggerDagRunSpec>([
  "dagId",
  "runId",
  "logicalDate",
  "runAfter",
  "conf",
  "resetDagRun",
  "waitForCompletion",
  "pokeInterval",
  "allowedStates",
  "failedStates",
  "skipWhenAlreadyExists",
  "failWhenDagIsPaused",
  "note",
  "deferrable",
]);

function checkStates(name: string, states: unknown): DagRunState[] | undefined {
  if (states === undefined) return undefined;
  if (!Array.isArray(states)) {
    throw new Error(`triggerDagRun(...) option "${name}" must be an array of Dag run states`);
  }
  for (const state of states) {
    if (typeof state !== "string" || !DAG_RUN_STATES.has(state)) {
      throw new Error(
        `triggerDagRun(...) option "${name}" holds ${JSON.stringify(state)}, which is not a Dag ` +
          `run state; use one of ${[...DAG_RUN_STATES].join(", ")}`,
      );
    }
  }
  return [...(states as DagRunState[])];
}

function checkType(name: string, value: unknown, type: "string" | "boolean"): void {
  if (value !== undefined && typeof value !== type) {
    throw new Error(`triggerDagRun(...) option "${name}" must be a ${type}`);
  }
}

function checkDate(name: string, value: unknown, allowNull = false): void {
  if (value === undefined || (allowNull && value === null)) return;
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    throw new Error(`triggerDagRun(...) option "${name}" must be a valid Date`);
  }
}

/**
 * A task that triggers another Dag's run, passed to `dag.task` in place of a handler.
 *
 * ```ts
 * dag.task(triggerDagRun({ dagId: "downstream_etl" }), { taskId: "trigger_downstream" })();
 * ```
 */
export function triggerDagRun(spec: TriggerDagRunSpec): TriggerDagRunTask {
  const options: unknown = spec;
  if (!isPlainRecord(options)) {
    throw new Error("triggerDagRun(...) takes an options object");
  }
  for (const name of Object.keys(options)) {
    if (!OPTION_NAMES.has(name)) throw new Error(`Unknown option "${name}" for triggerDagRun(...)`);
  }
  if (typeof options["dagId"] !== "string" || options["dagId"].length === 0) {
    throw new Error("triggerDagRun(...) needs the dagId of the Dag to trigger");
  }
  const conf = options["conf"];
  if (conf !== undefined && (typeof conf !== "object" || conf === null || Array.isArray(conf))) {
    throw new Error('triggerDagRun(...) option "conf" must be an object');
  }
  checkType("runId", options["runId"], "string");
  checkType("note", options["note"], "string");
  checkDate("logicalDate", options["logicalDate"], true);
  checkDate("runAfter", options["runAfter"]);
  for (const flag of [
    "resetDagRun",
    "waitForCompletion",
    "skipWhenAlreadyExists",
    "failWhenDagIsPaused",
    "deferrable",
  ]) {
    checkType(flag, options[flag], "boolean");
  }
  const pokeInterval = options["pokeInterval"] ?? 60;
  if (typeof pokeInterval !== "number" || !Number.isFinite(pokeInterval) || pokeInterval < 0) {
    throw new Error('triggerDagRun(...) option "pokeInterval" must be a non-negative number');
  }
  const allowedStates = checkStates("allowedStates", options["allowedStates"]);
  const failedStates = checkStates("failedStates", options["failedStates"]);
  const task: TriggerDagRunTask = {
    dagId: options["dagId"],
    runId: (options["runId"] as string | undefined) || undefined,
    logicalDate: options["logicalDate"] as Date | null | undefined,
    runAfter: options["runAfter"] as Date | undefined,
    conf: conf as TriggerDagRunTask["conf"],
    resetDagRun: options["resetDagRun"] === true,
    waitForCompletion: options["waitForCompletion"] === true,
    pokeInterval,
    // An empty `failedStates` is kept, as Python keeps it.
    allowedStates: allowedStates?.length ? allowedStates : ["success"],
    failedStates: failedStates ?? ["failed"],
    skipWhenAlreadyExists: options["skipWhenAlreadyExists"] === true,
    failWhenDagIsPaused: options["failWhenDagIsPaused"] === true,
    note: (options["note"] as string | undefined) || undefined,
    deferrable:
      (options["deferrable"] as boolean | undefined) ??
      getBooleanEnv("AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE", false),
    operatorName: "TriggerDagRunOperator",
    requiresTaskId: true,
    label: "triggerDagRun",
    taskIdExample: 'dag.task(triggerDagRun({ dagId: "..." }), { taskId: "trigger_downstream" })',
    takesNoInputs: true,
    serialize: (label) => {
      if (task.conf !== undefined) toPlainJson(task.conf, `conf of ${label}`);
      // Makes the UI draw the task as `TriggerDagRunOperator` with its link.
      return {
        ui_color: "#ffefeb",
        _operator_extra_links: { "Triggered DAG": "_link_TriggerDagRunLink" },
      };
    },
    getDagDependencies: () => [{ target: task.dagId, dependencyType: "trigger" }],
    execute: (op) => executeTriggerDagRun(task, op),
    executeComplete: (op, event) => resumeTriggerDagRun(task, op, event),
  };
  return brandOperator(task);
}
