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

// The Dag authoring surface: `new Dag(dagId)` plus `dag.task(taskId, handler)`.

import { brand, hasBrand } from "./brand.js";
import type { TaskHandler } from "./task.js";

// Mirrors the Python task-SDK KEY_REGEX and validate_key in airflow.sdk.definitions._internal.node.
const KEY_REGEX = /^[\p{L}\p{N}_.-]+$/u;
const MAX_KEY_LENGTH = 250;

function validateKey(name: string, value: string): void {
  if (typeof value !== "string" || !KEY_REGEX.test(value)) {
    throw new Error(
      `${name} must be made of alphanumeric characters, dashes, dots, and underscores`,
    );
  }
  if (value.length > MAX_KEY_LENGTH) {
    throw new Error(`${name} must be less than ${MAX_KEY_LENGTH} characters, not ${value.length}`);
  }
}

function validateEmptySpec(name: string, value: unknown): void {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    Reflect.ownKeys(value).length > 0
  ) {
    throw new Error(`${name} must be an empty object`);
  }
}

/**
 * Dag-level options.
 *
 * No fields yet, so only `{}` is accepted: a field that would be silently
 * dropped — `new Dag("d", { schedule: "@daily" })` — is a compile error.
 *
 * Native Dag declaration will add optional fields here, generated from the
 * serialized-Dag JSON schema as `src/generated/supervisor.ts` is.
 */
export type DagSpec = Record<string, never>;

/**
 * Task-level options.
 *
 * No fields yet, so only `{}` is accepted, as with {@link DagSpec}.
 *
 * Future task fields (retries, ...) will land here.
 */
export type TaskSpec = Record<string, never>;

/**
 * A reference to a task registered on a {@link Dag}, returned by `dag.task(...)`.
 *
 * Identity only: the handler is deliberately not exposed.
 *
 * References are what `inputs` accepts, and what native Dag declaration will
 * use to wire dependencies.
 */
export interface TaskRef {
  /** Identifier of the Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
}

/**
 * Task references keyed by input name.
 *
 * Stored and validated, but they do not create dependencies or pass values yet
 * — see {@link TaskOptions.inputs}. Each must identify an earlier task in the
 * same Dag. Literal values are not supported.
 */
export type TaskInputs = Readonly<Record<string, TaskRef>>;

/**
 * Named options for `dag.task()`.
 *
 * Keyword-only so neither field has to be positioned around the other, and so
 * future fields can be added without a new parameter. Unknown keys are
 * rejected, so a typo fails at import time rather than being ignored.
 */
export interface TaskOptions {
  /**
   * References to the upstream tasks this task consumes.
   *
   * Not used yet: a handler receives `{ctx, client}` only, and the Python stub
   * Dag defines task order. Read an upstream return value explicitly instead —
   * `client.getXCom({ key: "return_value", taskId: "extract" })`, where
   * omitting `taskId` reads the *running* task's own XCom, not the upstream.
   *
   * In the future these will declare dependencies in native TypeScript Dags.
   */
  readonly inputs?: TaskInputs;
  /** Task-level options. Stored, but not used yet — see {@link TaskSpec}. */
  readonly spec?: TaskSpec;
}

/** Per-task record a Dag retains: the handle, the handler, its spec, and the
 *  upstream handles feeding it. */
export interface TaskRecord {
  readonly task: TaskRef;
  readonly handler: TaskHandler;
  readonly spec: TaskSpec;
  /** Upstream handles keyed by input name; empty when the task has no inputs. */
  readonly inputs: TaskInputs;
}

// Assigned inside Dag's static block: gives package-internal code read access
// to the #tasks private field without a public accessor on the Dag class.
let taskRecordsOf: (dag: Dag) => ReadonlyMap<string, TaskRecord>;

/** Internal: whether `value` is a Dag built by any copy of this package. */
export function isDag(value: unknown): value is Dag {
  return hasBrand(value, "Dag");
}

/**
 * A Dag declared in TypeScript.
 *
 * Today the Dag structure itself is still declared by a Python stub file; a
 * `Dag` instance binds TypeScript handlers to that stub's Dag/task IDs. The
 * instance retains its `spec` and every task's `(taskId, handler, spec)` so a
 * future `serialize()` can produce the serialized Dag JSON for native
 * TypeScript Dag declaration.
 *
 * Constructing a Dag has no effect beyond the instance itself. Collect the ones
 * a bundle should serve in a `DagRegistry` and pass it to `serveDags(...)`.
 */
export class Dag {
  /** Identifier of this Dag. Must match the Python Dag's `dag_id`. */
  readonly dagId: string;
  /** Dag-level options this instance was constructed with, copied and frozen. */
  readonly spec: DagSpec;
  readonly #tasks = new Map<string, TaskRecord>();

  static {
    taskRecordsOf = (dag) => dag.#tasks;
  }

  constructor(dagId: string, spec: DagSpec = {}) {
    validateKey("dagId", dagId);
    validateEmptySpec(`spec for Dag "${dagId}"`, spec);
    brand(this, "Dag");
    this.dagId = dagId;
    // Copied and frozen, as task specs and inputs are: nothing reads a spec
    // until the bundle manifest is built, long after the user's module has run,
    // so a later mutation of their object would silently change what is packed.
    // Shallow — a nested value in a future generated spec stays mutable.
    this.spec = Object.freeze({ ...spec });
  }

  /** Task IDs attached to this Dag, in attachment order. */
  get taskIds(): readonly string[] {
    return [...this.#tasks.keys()];
  }

  /**
   * Register a TypeScript handler for a task of this Dag.
   *
   * `taskId` must match the Dag-side operator's `task_id` exactly, including
   * any TaskGroup prefix. Returns this task's handle.
   */
  task<TReturn = unknown>(
    taskId: string,
    handler: TaskHandler<TReturn>,
    options: TaskOptions = {},
  ): TaskRef {
    validateKey("taskId", taskId);
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    if (this.#tasks.has(taskId)) {
      throw new Error(`Task "${taskId}" is already registered for Dag "${this.dagId}"`);
    }
    this.#validateOptions(taskId, options);
    const { inputs = {}, spec = {} } = options;
    validateEmptySpec(`spec for Dag "${this.dagId}" task "${taskId}"`, spec);
    this.#validateInputs(taskId, inputs);
    const task: TaskRef = Object.freeze({ dagId: this.dagId, taskId });
    this.#tasks.set(taskId, {
      task,
      handler: handler as TaskHandler,
      spec: Object.freeze({ ...spec }),
      inputs: Object.freeze({ ...inputs }),
    });
    return task;
  }

  // TypeScript is bypassable — from plain JavaScript, or an `as TaskOptions`
  // cast — so an unknown key is rejected rather than silently ignored.
  #validateOptions(taskId: string, options: TaskOptions): void {
    const value: unknown = options;
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new Error(`options for Dag "${this.dagId}" task "${taskId}" must be an object`);
    }
    for (const key of Object.keys(value)) {
      if (key !== "inputs" && key !== "spec") {
        throw new Error(`Unknown option "${key}" for Dag "${this.dagId}" task "${taskId}"`);
      }
    }
  }

  #validateInputs(taskId: string, inputs: TaskInputs): void {
    for (const [name, upstream] of Object.entries(inputs)) {
      if (
        upstream == null ||
        typeof upstream.dagId !== "string" ||
        typeof upstream.taskId !== "string"
      ) {
        throw new Error(
          `Input "${name}" of task "${taskId}" must be a task handle returned by dag.task(...)`,
        );
      }
      if (upstream.dagId !== this.dagId) {
        throw new Error(
          `Input "${name}" of task "${taskId}" comes from Dag "${upstream.dagId}", not "${this.dagId}"`,
        );
      }
      // An input can only name a task registered earlier on this Dag, which
      // makes self-references and cycles unrepresentable.
      if (!this.#tasks.has(upstream.taskId)) {
        throw new Error(
          `Input "${name}" of task "${taskId}" refers to unregistered task "${upstream.taskId}"`,
        );
      }
    }
  }
}

/**
 * Internal: the task records of a Dag, for registry lookups.
 *
 * Not re-exported from the package root, and the package `"exports"` map
 * blocks deep imports, so this is unreachable from outside the SDK.
 */
export function getDagTaskRecords(dag: Dag): ReadonlyMap<string, TaskRecord> {
  return taskRecordsOf(dag);
}
