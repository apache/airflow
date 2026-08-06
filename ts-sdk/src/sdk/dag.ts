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

/**
 * Dag-level options.
 *
 * Empty today: native TypeScript Dag declaration (schedule, tags, ...) will add
 * optional fields here without changing the `Dag` constructor signature.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type -- extension point for future native-Dag fields
export interface DagSpec {}

/**
 * Task-level options.
 *
 * Empty today: future task fields (retries, ...) will land here without
 * changing the `dag.task()` signature.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type -- extension point for future task fields
export interface TaskSpec {}

/**
 * Opaque handle to a task registered on a {@link Dag}.
 *
 * Pass it as a downstream task's input to declare that the downstream task
 * consumes this task's return value:
 *
 * ```ts
 * const extracted = dag.task("extract", extractFn);
 * const transformed = dag.task("transform", transformFn, { inputs: { extracted } });
 * dag.task("load", loadFn, { inputs: { transformed } });
 * ```
 *
 * In today's Python-stub mode the stub Dag still defines task order; declared
 * inputs are retained for the serialized Dag JSON that native TypeScript Dag
 * declaration will emit. The handler is intentionally not exposed on the handle.
 */
export interface Task {
  /** Identifier of the Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
}

/**
 * Upstream task handles a task consumes, keyed by input name.
 *
 * Values must be handles returned by `dag.task(...)`. Literal values are
 * deliberately out of scope for now; the future native-Dag work decides how
 * they are declared.
 */
export type TaskInputs = Readonly<Record<string, Task>>;

/**
 * Named options for `dag.task()`.
 *
 * Keyword-only so neither field has to be positioned around the other, and so
 * future fields can be added without a new parameter.
 */
export interface TaskOptions {
  /** Upstream task handles whose return values this task consumes. */
  readonly inputs?: TaskInputs;
  /** Task-level options. */
  readonly spec?: TaskSpec;
}

/** Per-task record a Dag retains: the handle, the handler, its spec, and the
 *  upstream handles feeding it. */
export interface TaskRecord {
  readonly task: Task;
  readonly handler: TaskHandler;
  readonly spec: TaskSpec;
  /** Upstream handles keyed by input name; empty when the task has no inputs. */
  readonly inputs: TaskInputs;
}

// Assigned inside Dag's static block: gives package-internal code read access
// to the #tasks private field without a public accessor on the Dag class.
let taskRecordsOf: (dag: Dag) => ReadonlyMap<string, TaskRecord>;

/**
 * A Dag declared in TypeScript.
 *
 * Today the Dag structure itself is still declared by a Python stub file; a
 * `Dag` instance binds TypeScript handlers to that stub's Dag/task IDs. The
 * instance retains its `spec` and every task's `(taskId, handler, spec)` so a
 * future `serialize()` can produce the serialized Dag JSON for native
 * TypeScript Dag declaration.
 */
export class Dag {
  /** Identifier of this Dag. Must match the Python Dag's `dag_id`. */
  readonly dagId: string;
  /** Dag-level options this instance was constructed with. */
  readonly spec: DagSpec;
  readonly #tasks = new Map<string, TaskRecord>();

  static {
    taskRecordsOf = (dag) => dag.#tasks;
  }

  constructor(dagId: string, spec: DagSpec = {}) {
    validateKey("dagId", dagId);
    this.dagId = dagId;
    this.spec = spec;
  }

  /**
   * Register a TypeScript handler for a task of this Dag.
   *
   * `taskId` must match the Dag-side operator's `task_id` exactly, including
   * any TaskGroup prefix. `options.inputs` names the upstream task handles whose
   * return values this task consumes. Returns this task's handle.
   */
  task<TReturn = unknown>(
    taskId: string,
    handler: TaskHandler<TReturn>,
    options: TaskOptions = {},
  ): Task {
    const { inputs = {}, spec = {} } = options;
    validateKey("taskId", taskId);
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    if (this.#tasks.has(taskId)) {
      throw new Error(`Task "${taskId}" is already registered for Dag "${this.dagId}"`);
    }
    this.#validateInputs(taskId, inputs);
    const task: Task = Object.freeze({ dagId: this.dagId, taskId });
    this.#tasks.set(taskId, {
      task,
      handler: handler as TaskHandler,
      spec,
      inputs: Object.freeze({ ...inputs }),
    });
    return task;
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
 * Internal: the task records of a Dag, for registry lookups and manifests.
 *
 * Not re-exported from the package root, and the package `"exports"` map
 * blocks deep imports, so this is unreachable from outside the SDK.
 */
export function getDagTaskRecords(dag: Dag): ReadonlyMap<string, TaskRecord> {
  return taskRecordsOf(dag);
}
