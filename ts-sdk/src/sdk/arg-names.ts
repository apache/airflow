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

// Stating a binding explicitly.

import type { TaskFunction } from "./task.js";

/**
 * A rename per argument, keyed by the handler's own parameter names.
 *
 * Every key is checked against the handler's parameter type, so a typo is a
 * compile error naming the right key. The values are Python names, which `tsc`
 * cannot see and does not check.
 */
export type ArgNameMap<TArgs> = {
  readonly [K in keyof TArgs]?: string;
};

// Read back off the handler at the dispatch site. A global symbol, as the brands are:
// two resolved copies must agree on the key, or a handler wrapped by one loses its renames.
const ARG_NAMES = Symbol.for("airflow.ts-sdk.arg-names");

function validate(names: ArgNameMap<unknown>): ReadonlyMap<string, string> {
  const candidate: unknown = names;
  if (
    typeof candidate !== "object" ||
    candidate === null ||
    Array.isArray(candidate) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(candidate) as object | null)
  ) {
    throw new Error("withArgNames(...) takes a plain object mapping argument names to wire names");
  }
  const entries = new Map<string, string>();
  for (const [key, value] of Object.entries(candidate as Record<string, unknown>)) {
    if (typeof value !== "string" || value.length === 0) {
      throw new Error(
        `withArgNames(...) maps "${key}" to ${JSON.stringify(value)}; a wire name must be a non-empty string`,
      );
    }
    entries.set(key, value);
  }
  return entries;
}

/**
 * Bind an argument explicitly, naming it on both sides.
 *
 * Mapping first, handler second. An entry takes precedence over folding, and
 * everything the map does not mention still folds:
 *
 * ```ts
 * interface ReportArgs {
 *   label: string; // Python calls this `run_label`
 *   threshold: number;
 * }
 *
 * const report = withArgNames({ label: "run_label" }, async ({ label, threshold }: ReportArgs) => {
 *   // `label` is the call's `run_label`; `threshold` folded as usual.
 * });
 *
 * bundle.register(new TaskHandler("etl", "report", report));
 * ```
 *
 * Folding absorbs ordinary spelling differences, so this is for a name that genuinely differs:
 * a clearer word than the Dag chose, or a TypeScript reserved word like `enum`.
 * It should be rare in a real Dag.
 *
 * The map's keys are checked against the handler's own parameter type,
 * so `{ labl: "run_label" }` is a compile error.
 * Its values are Python names, which `tsc` cannot check.
 */
export function withArgNames<TArgs, TReturn>(
  // NoInfer, so `TArgs` comes from the handler alone:
  // inferring it from the map too would make every key correct and check nothing.
  names: ArgNameMap<NoInfer<TArgs>>,
  handler: TaskFunction<TArgs, TReturn>,
): TaskFunction<TArgs, TReturn> {
  const resolved = validate(names);
  if (typeof handler !== "function") {
    throw new Error("withArgNames(...) takes the handler function as its second argument");
  }
  // A wrapper rather than a property on the author's own function:
  // one handler can be registered for two tasks that rename differently.
  const wrapped: TaskFunction<TArgs, TReturn> = (args) => handler(args);
  Object.defineProperty(wrapped, ARG_NAMES, { value: resolved });
  return wrapped;
}

/** Internal: the renames a handler was wrapped with, empty when it has none. */
export function getArgNames(handler: TaskFunction<never, unknown>): ReadonlyMap<string, string> {
  const carrier = handler as unknown as Record<symbol, unknown>;
  const names = carrier[ARG_NAMES];
  return names instanceof Map ? (names as ReadonlyMap<string, string>) : EMPTY_ARG_NAMES;
}

const EMPTY_ARG_NAMES: ReadonlyMap<string, string> = new Map();
