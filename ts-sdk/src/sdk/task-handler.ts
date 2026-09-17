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

// The mixed-language authoring surface: a TypeScript function bound to the
// Python-owned task it implements.

import { brand, hasBrand } from "./brand.js";
import type { TaskFunction } from "./task.js";

// Assigned inside TaskHandler's static block, as Dag does for its tasks.
let functionOf: (handler: TaskHandler) => TaskFunction;

/** Internal: whether `value` is a TaskHandler built by any copy of this package. */
export function isTaskHandler(value: unknown): value is TaskHandler {
  return hasBrand(value, "TaskHandler");
}

function requireId(label: string, value: string): string {
  // Typed as string, so plain JavaScript is what these catch, along with the
  // empty string, which types cannot rule out and which no Airflow id can be.
  const candidate: unknown = value;
  if (typeof candidate !== "string" || candidate.length === 0) {
    throw new Error(`${label} for a task handler must be a non-empty string`);
  }
  return value;
}

/**
 * A TypeScript function bound to the Python-owned task it implements.
 *
 * A mixed-language Dag declares its structure in Python, with a `@task.stub`
 * per task routed to the Node coordinator. TypeScript supplies the task bodies
 * and nothing else: no dag_id of its own, no schedule, no task order.
 *
 * ```ts
 * const bundle = new Bundle();
 * bundle.register(new TaskHandler("etl", "transform", transform));
 * await bundle.serve();
 * ```
 *
 * `dagId` must match the Python Dag's `dag_id` and `taskId` a `@task.stub` in
 * it, including any TaskGroup prefix. Both are written out: nothing is derived
 * from the handler's function name, which the build step is free to rename.
 *
 * Identity and a body, and no more. A handler has no factory to call, so wiring
 * one the way a natively declared task is wired is a compile error rather than
 * a runtime throw. For a native Dag, use {@link Dag} instead.
 */
export class TaskHandler<TReturn = unknown> {
  /** Identifier of the Python Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID this handler implements, including any TaskGroup prefix. */
  readonly taskId: string;
  readonly #handler: TaskFunction<TReturn>;

  static {
    functionOf = (handler) => handler.#handler as TaskFunction;
  }

  constructor(dagId: string, taskId: string, handler: TaskFunction<TReturn>) {
    this.dagId = requireId("dagId", dagId);
    this.taskId = requireId("taskId", taskId);
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${dagId}" task "${taskId}" must be a function`);
    }
    brand(this, "TaskHandler");
    this.#handler = handler;
  }
}

/**
 * Internal: the function a TaskHandler carries, for bundle dispatch.
 *
 * Not re-exported from the package root, and the package `"exports"` map blocks
 * deep imports, so this is unreachable from outside the SDK. The field is
 * private for the same reason `TaskRef` does not expose its handler: what a
 * handler binds is identity, and reaching the body is the runtime's business.
 */
export function getTaskHandlerFunction(handler: TaskHandler): TaskFunction {
  return functionOf(handler);
}
