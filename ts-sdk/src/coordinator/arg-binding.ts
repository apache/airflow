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

// The TaskFlow call arguments Airflow captured for a task, delivered by name.
//
// A Python Dag declares a task that runs in another language with `@task.stub`
// and calls it TaskFlow-style, as in `transform("uk", 0.75)`. Airflow
// materializes that call site into an ordered, named spec and delivers it as
// `StartupDetails.ti_context.arg_bindings`; this turns it back into the object
// the handler destructures.
//
// Names bind by folding on both sides, so nothing has to be declared for a
// Python `region_code` to reach a handler's `regionCode`.

import type { LogChannel } from "./log-channel.js";
import type { ArgBindings, TaskArgBinding } from "../generated/supervisor.js";
import type { JsonValue } from "../sdk/client-types.js";

/**
 * Fold a name to the token both sides are matched on.
 *
 * Identical to the Go SDK's `strings.ToLower(strings.ReplaceAll(name, "_", ""))`,
 * so one Python signature binds the same way in either SDK. Only `_` is
 * removed, because it is the only separator a Python parameter name can
 * contain.
 */
export function foldArgName(name: string): string {
  return name.replaceAll("_", "").toLowerCase();
}

/** A task's TaskFlow call arguments, ready to hand to its handler. */
export interface BoundArgs {
  /**
   * The object the handler is called with, a Proxy over the wire names: reading
   * a name folds it on demand, so `regionCode` and `region_code` both reach
   * Python's `region_code`.
   */
  readonly args: object;
  /**
   * Python's names, in the calling signature's declaration order. What
   * `Object.keys(args)` yields, and what a failing task reports.
   */
  readonly names: readonly string[];
}

const EMPTY_ARGS: BoundArgs = { args: Object.freeze({}), names: Object.freeze([]) };

/**
 * Decode `ti_context.arg_bindings` into the object a task handler receives.
 *
 * A duplicate fold fails the task here, before the handler runs: two Python
 * names that fold to one token cannot both be reached, and picking either
 * silently would hand the handler the wrong value.
 */
export function bindArgs(bindings: ArgBindings | undefined, logs: LogChannel): BoundArgs {
  // Absent for an Airflow too old to send a spec, and for a task called with no
  // arguments. Both mean the handler's parameter has nothing in it.
  if (bindings == null || bindings.length === 0) return EMPTY_ARGS;

  const names: string[] = [];
  const byFold = new Map<string, string>();
  const values = new Map<string, JsonValue>();
  for (const binding of bindings) {
    const name = binding.name;
    const fold = foldArgName(name);
    const clash = byFold.get(fold);
    if (clash !== undefined) {
      throw new Error(
        `Task arguments "${clash}" and "${name}" both fold to "${fold}", so a handler cannot ` +
          "name them apart; rename one in the @task.stub signature",
      );
    }
    byFold.set(fold, name);
    names.push(name);
    values.set(name, resolveBindingValue(binding));
  }

  return { args: makeArgsProxy(names, byFold, values, logs), names };
}

/** The value a binding carries, or a throw for one this SDK cannot honour.
 *
 *  A binding that cannot be honoured fails the task rather than being dropped:
 *  an unbound argument reaches the handler as `undefined`, which corrupts the
 *  task's output instead of stopping it. */
function resolveBindingValue(binding: TaskArgBinding): JsonValue {
  const { name } = binding;
  if (binding.kind === "literal") {
    // Airflow omits `value` for a literal whose value is null.
    return (binding.value ?? null) as JsonValue;
  }
  if (binding.kind === "xcom") {
    throw new Error(
      `Task argument "${name}" takes the output of upstream task "${binding.task_id}", but ` +
        "XCom-backed arguments are not supported yet; read the value inside the handler " +
        `instead, with getClient().getXCom({ key: "return_value", taskId: "${binding.task_id}" })`,
    );
  }
  // Unreachable for the wire union as generated, so `binding` is `never` here.
  // A newer Airflow can add a kind, and skipping it would silently leave the
  // argument unbound, so the kind is read back off the value.
  const kind: unknown = (binding as { kind?: unknown }).kind;
  throw new Error(
    `Task argument "${name}" has binding kind ${JSON.stringify(kind)}, which this version ` +
      "of apache-airflow-ts-sdk cannot bind; upgrade it to match this Airflow release",
  );
}

/**
 * The object a handler destructures.
 *
 * A Proxy rather than a pre-built object with both spellings, because the SDK
 * has no way to know which spelling a handler will use: it sees Python's names
 * and nothing else. Folding on read means binding needs nothing declared on
 * either side, and no guess about the TypeScript name is ever materialized.
 */
function makeArgsProxy(
  names: readonly string[],
  byFold: ReadonlyMap<string, string>,
  values: ReadonlyMap<string, JsonValue>,
  logs: LogChannel,
): object {
  const resolve = (property: string): string | undefined =>
    values.has(property) ? property : byFold.get(foldArgName(property));

  // A null prototype so a read never reaches Object.prototype: a Python
  // argument named `constructor` or `toString` must bind like any other, and a
  // handler destructuring one that was not passed must miss rather than get a
  // function.
  return new Proxy(Object.create(null) as Record<string, unknown>, {
    get(_target, property) {
      // Only string keys are arguments. A symbol read is something else, such
      // as `Symbol.toPrimitive` during string coercion, and is not a miss.
      if (typeof property !== "string") return undefined;
      const name = resolve(property);
      if (name !== undefined) return values.get(name);
      // Logged, never thrown: a destructuring default such as
      // `{ runId = "manual" }` is a legitimate miss, and nothing here can tell
      // one from a typo.
      logs.warning("Task argument not bound by this task's call", {
        requested: property,
        bound: [...names],
      });
      return undefined;
    },
    has(_target, property) {
      // `in` folds like a read, so `"regionCode" in args` answers for the
      // Python `region_code` the handler would actually receive.
      return typeof property === "string" && resolve(property) !== undefined;
    },
    ownKeys() {
      // Python's names: the SDK has no TypeScript-side names to enumerate, so
      // `Object.keys` and rest destructuring (`{ ...rest }`) report what the
      // wire actually delivered.
      return [...names];
    },
    getOwnPropertyDescriptor(_target, property) {
      if (typeof property !== "string" || !values.has(property)) return undefined;
      // Enumerable and configurable, or `ownKeys` would throw an invariant
      // error for a key the target itself does not have.
      return {
        value: values.get(property),
        writable: false,
        enumerable: true,
        configurable: true,
      };
    },
    set(_target, property) {
      throw new Error(
        `Cannot assign to task argument ${String(property)}: bound arguments are read-only`,
      );
    },
    deleteProperty(_target, property) {
      throw new Error(
        `Cannot delete task argument ${String(property)}: bound arguments are read-only`,
      );
    },
  });
}
