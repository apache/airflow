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

// The task-handler call surface: what a task handler sees while it runs.
//
// Everything the SDK supplies comes from a getter rather than a parameter, so
// nothing it injects shares a namespace with an author's arguments.

import { AsyncLocalStorage } from "node:async_hooks";

import type { TaskClient } from "./client.js";

/** Runtime metadata for the current task invocation. */
export interface TaskContext {
  /** Identifier of the Dag containing this task. */
  readonly dagId: string;
  /** Task ID for this handler invocation, including any TaskGroup prefix. */
  readonly taskId: string;
  /** Dag run identifier for the current task attempt. */
  readonly runId: string;
  /** Airflow try number for the current task attempt. */
  readonly tryNumber: number;
  /** -1 for non-mapped tasks, 0..N-1 for mapped instances. */
  readonly mapIndex: number;
  /**
   * AbortSignal that fires when Airflow terminates the task subprocess
   * with SIGTERM or SIGINT.
   *
   * Pass this signal to `fetch()`, timers, or any other API that accepts an
   * abort signal for cooperative cancellation and cleanup.
   */
  readonly signal: AbortSignal;
}

/** What the runtime puts in scope for the duration of one handler call. */
export interface TaskScope {
  readonly ctx: TaskContext;
  readonly client: TaskClient;
}

// Keyed on a global symbol, as the serve latch is: two resolved copies of the
// package would otherwise hold one storage each, and a handler reaching for
// `getClient()` through the copy that is not running the task would find
// nothing in scope.
const SCOPE_STORAGE = Symbol.for("airflow.ts-sdk.task-scope");

function scopeStorage(): AsyncLocalStorage<TaskScope> {
  const holder = globalThis as unknown as Record<symbol, AsyncLocalStorage<TaskScope> | undefined>;
  return (holder[SCOPE_STORAGE] ??= new AsyncLocalStorage<TaskScope>());
}

/**
 * Internal: call `fn` with `scope` in place, as the runtime does per task.
 *
 * `AsyncLocalStorage` carries the store across every `await` and into every
 * promise created inside `fn`, so a handler's helpers see it without being
 * passed anything. Not re-exported from the package root: a handler reads the
 * scope, it does not install one.
 */
export function runInTaskScope<T>(scope: TaskScope, fn: () => T): T {
  return scopeStorage().run(scope, fn);
}

type ScopeAccessor = "getContext" | "getClient";

function currentScope(accessor: ScopeAccessor): TaskScope {
  const scope = scopeStorage().getStore();
  if (!scope) {
    throw new Error(
      `${accessor}() is only available inside a task handler. ` +
        "The scope is in place only for the duration of the handler call, so " +
        "this ran either at module top level or in work that outlived it.",
    );
  }
  return scope;
}

/**
 * Runtime metadata for the task currently running.
 *
 * ```ts
 * async function transform() {
 *   throw new Error(`task ${getContext().taskId} has nothing to transform`);
 * }
 * ```
 *
 * @throws when called outside a task handler.
 */
export function getContext(): TaskContext {
  return currentScope("getContext").ctx;
}

/**
 * Client for reading and writing Airflow task-time data for the task currently
 * running.
 *
 * ```ts
 * async function transform() {
 *   const client = getClient();
 *   return await client.getXCom<number>({ key: "return_value", taskId: "extract" });
 * }
 * ```
 *
 * Work that outlives the handler is the one gap. Async context propagates into
 * a promise created inside the handler, so one it never awaits still resolves,
 * but it runs after the task's terminal state has been reported and writes to
 * a finished task. Await everything a handler starts.
 *
 * @throws when called outside a task handler.
 */
export function getClient(): TaskClient {
  return currentScope("getClient").client;
}

/**
 * Function signature for a TypeScript task handler.
 *
 * `TArgs` describes what the Dag's call site passes. {@link getContext} and
 * {@link getClient} reach the runtime from inside the call, so neither is a
 * parameter.
 *
 * ```ts
 * interface TransformArgs {
 *   regionCode: string;
 *   threshold: number;
 * }
 *
 * async function transform({ regionCode, threshold }: TransformArgs) {
 *   // ...
 * }
 * ```
 *
 * A handler that takes no arguments declares no parameter.
 *
 * Non-`undefined` return values are automatically pushed to XCom under the
 * `"return_value"` key, matching Python `@task` behavior.
 */
export type TaskFunction<TArgs = void, TReturn = unknown> = (
  args: TArgs,
) => TReturn | Promise<TReturn>;
