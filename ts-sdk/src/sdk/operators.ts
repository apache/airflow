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

// Operators a TypeScript Dag can declare but not run.
//
// A task that wraps a TypeScript function executes in this runtime; one of
// these carries no body at all. It serializes into the Dag as the Python
// operator it names, and a Python worker executes it — which is why the SDK
// needs no deferral mechanism to offer `waitForCompletion` or `deferrable`.
//
// `triggerDagRun` is the hand-written member of that family. The rest are
// generated from the provider constructors, as
// `airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md`
// describes; this one is written by hand because triggering a Dag run is
// control flow rather than a provider integration
// (`airflow-core/adr/lang-sdk/0008-control-flow-constructs.md`, decision 4).

import { brand, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";

/**
 * A task that serializes as a Python operator and carries no TypeScript body.
 *
 * Opaque: what it holds is the serializer's business, and an author only ever
 * hands one to `dag.task(...)`.
 */
export interface OperatorRef {
  /** Python class the serialized task names, e.g. `TriggerDagRunOperator`. */
  readonly taskType: string;
  /** Module that class is imported from, as `_task_module` records it. */
  readonly taskModule: string;
  /** Constructor arguments, already under their Python keyword names. */
  readonly args: Readonly<Record<string, JsonValue>>;
}

/** Internal: whether `value` is an operator built by any copy of this package. */
export function isOperatorRef(value: unknown): value is OperatorRef {
  return hasBrand(value, "OperatorRef");
}

/** The `TriggerDagRunOperator` options, named as TypeScript spells them. */
export interface TriggerDagRunOptions {
  /** Identifier of the Dag to trigger. */
  readonly dagId: string;
  /** Run ID for the triggered run; Airflow generates one when unset. */
  readonly runId?: string;
  /** Configuration the triggered run is started with. */
  readonly conf?: Readonly<Record<string, JsonValue>>;
  /** Clear an existing run with the same ID instead of failing. */
  readonly resetDagRun?: boolean;
  /** Hold this task open until the triggered run finishes. */
  readonly waitForCompletion?: boolean;
  /** Seconds between checks while waiting. */
  readonly pokeInterval?: number;
  /** Run states that count as success when waiting. */
  readonly allowedStates?: readonly string[];
  /** Run states that count as failure when waiting. */
  readonly failedStates?: readonly string[];
  /** Skip rather than fail when the run already exists. */
  readonly skipWhenAlreadyExists?: boolean;
  /** Fail rather than trigger when the target Dag is paused. */
  readonly failWhenDagIsPaused?: boolean;
  /** Note recorded against the triggered run. */
  readonly note?: string;
  /**
   * Free the worker slot while waiting.
   *
   * Handled by the Python task runner, which is what runs this task; the
   * TypeScript SDK needs no deferral mechanism of its own to offer it.
   */
  readonly deferrable?: boolean;
}

/**
 * What `dag.triggerDagRun(...)` takes: the operator's options, plus the task ID
 * the trigger takes in the Dag that declares it.
 */
export interface TriggerDagRunSpec extends TriggerDagRunOptions {
  /** Airflow task ID of the trigger task itself. */
  readonly taskId: string;
}

const TRIGGER_DAG_RUN_TYPE = "TriggerDagRunOperator";
const TRIGGER_DAG_RUN_MODULE = "airflow.providers.standard.operators.trigger_dagrun";

// Authoring name to Python keyword. Only the keywords the operator's
// constructor declares: an unknown one would reach the Python worker as a
// TypeError when the Dag runs, long after it parsed.
const TRIGGER_DAG_RUN_KEYWORDS: Readonly<Record<keyof TriggerDagRunOptions, string>> = {
  dagId: "trigger_dag_id",
  runId: "trigger_run_id",
  conf: "conf",
  resetDagRun: "reset_dag_run",
  waitForCompletion: "wait_for_completion",
  pokeInterval: "poke_interval",
  allowedStates: "allowed_states",
  failedStates: "failed_states",
  skipWhenAlreadyExists: "skip_when_already_exists",
  failWhenDagIsPaused: "fail_when_dag_is_paused",
  note: "note",
  deferrable: "deferrable",
};

/**
 * Internal: `Dag.triggerDagRun` is the authoring surface, and it strips the
 * `taskId` before calling this, so what arrives here is the operator's own
 * options.
 */
export function triggerDagRun(spec: TriggerDagRunOptions): OperatorRef {
  const value: unknown = spec;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("triggerDagRun(...) takes an options object");
  }
  const options = value as Record<string, unknown>;
  if (typeof options["dagId"] !== "string" || options["dagId"].length === 0) {
    throw new Error("triggerDagRun(...) needs the dagId of the Dag to trigger");
  }
  const args: Record<string, JsonValue> = {};
  for (const [name, option] of Object.entries(options)) {
    const keyword = TRIGGER_DAG_RUN_KEYWORDS[name as keyof TriggerDagRunOptions];
    if (keyword === undefined) {
      throw new Error(`Unknown option "${name}" for triggerDagRun(...)`);
    }
    // An option left unset is one the Python operator defaults for itself.
    if (option === undefined) continue;
    args[keyword] = option as JsonValue;
  }
  const operator: OperatorRef = {
    taskType: TRIGGER_DAG_RUN_TYPE,
    taskModule: TRIGGER_DAG_RUN_MODULE,
    args: Object.freeze(args),
  };
  brand(operator, "OperatorRef");
  return Object.freeze(operator);
}
