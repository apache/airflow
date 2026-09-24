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
// and calls it TaskFlow-style, as in `transform("uk", extract())`. Airflow
// materializes that call site into an ordered, named spec and delivers it as
// `StartupDetails.ti_context.arg_bindings`; this turns it back into the object
// the handler destructures, pulling any upstream output the call passed.
//
// Names bind by folding on both sides, so nothing has to be declared for a
// Python `region_code` to reach a handler's `regionCode`.

import type { CoordinatorClient } from "./client.js";
import type { LogChannel } from "./log-channel.js";
import type {
  ArgBindings,
  ArgValueSchema,
  TaskArgBinding,
  XComArgBinding,
} from "../generated/supervisor.js";
import type { JsonValue } from "../sdk/client-types.js";

/** The key an upstream task's return value is stored under, as Python `@task` does. */
const RETURN_VALUE_KEY = "return_value";

/** What Airflow stamps on an argument the Dag declared as a Python `int`. */
const INT64_FORMAT = "int64";

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

/** What {@link resolveArgs} needs beyond the spec itself. */
export interface ArgBindingDeps {
  readonly client: CoordinatorClient;
  /** The task's abort signal, so a terminated task stops mid-pull. */
  readonly signal: AbortSignal;
  readonly logs: LogChannel;
  /** Renames the handler declared with `withArgNames`, which beat folding. */
  readonly argNames: ReadonlyMap<string, string>;
}

/**
 * Resolve `ti_context.arg_bindings` into the object a task handler receives.
 *
 * A binding this SDK cannot honour fails the task before the handler can write anything.
 */
export async function resolveArgs(
  bindings: ArgBindings | undefined,
  deps: ArgBindingDeps,
): Promise<BoundArgs> {
  // Absent for an Airflow too old to send a spec, and for a task called with no
  // arguments. Both mean the handler's parameter has nothing in it.
  if (bindings == null || bindings.length === 0) return EMPTY_ARGS;

  // Checked in full before anything is pulled, leaving no half-resolved call behind.
  const names: string[] = [];
  const byFold = new Map<string, string>();
  let pullsUpstream = false;
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
    checkBindable(binding);
    pullsUpstream ||= binding.kind === "xcom";
  }

  // Every upstream pull in flight at once, so a task called with four of them
  // waits for one round-trip rather than four.
  const resolveAll = (): Promise<[string, JsonValue][]> =>
    Promise.all(
      bindings.map(async (binding): Promise<[string, JsonValue]> => [
        binding.name,
        binding.kind === "xcom"
          ? await pullXComArg(binding, deps.client)
          : literalValue(binding.value),
      ]),
    );
  // Literals need no request, so only a call that pulls races the abort signal.
  const entries = pullsUpstream ? await abortable(resolveAll, deps.signal) : await resolveAll();

  return { args: makeArgsProxy(names, byFold, new Map(entries), deps), names };
}

/** Airflow omits `value` for a literal whose value is null. */
function literalValue(value: unknown): JsonValue {
  return (value ?? null) as JsonValue;
}

/** Refuse a binding this SDK cannot honour, before any of them is resolved. */
function checkBindable(binding: TaskArgBinding): void {
  const { name } = binding;
  if (binding.kind === "xcom") return;
  if (binding.kind === "literal") {
    checkExactInteger(name, literalValue(binding.value), binding.value_schema);
    return;
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

/** Pull the upstream task's output this argument was called with. */
async function pullXComArg(binding: XComArgBinding, client: CoordinatorClient): Promise<JsonValue> {
  const entry = await client.getXComEntry({ key: RETURN_VALUE_KEY, taskId: binding.task_id });
  if (!entry.found) {
    throw new Error(
      `Task argument "${binding.name}" takes the output of upstream task "${binding.task_id}", ` +
        `which pushed no ${RETURN_VALUE_KEY} XCom; a task that returns nothing pushes none`,
    );
  }
  checkExactInteger(binding.name, entry.value, binding.value_schema);
  return entry.value;
}

/**
 * Refuse an integer JavaScript already failed to hold.
 *
 * A Python `int` outranges what a `number` represents exactly, so binding one
 * silently hands the handler a different value from the one the Dag produced.
 */
function checkExactInteger(
  name: string,
  value: JsonValue,
  schema: ArgValueSchema | null | undefined,
): void {
  if (schema?.["format"] !== INT64_FORMAT) return;
  if (typeof value !== "number" || Math.abs(value) <= Number.MAX_SAFE_INTEGER) return;
  throw new Error(
    `Task argument "${name}" is a 64-bit integer of ${value}, beyond the ` +
      `±${Number.MAX_SAFE_INTEGER} a JavaScript number holds exactly, so its low digits ` +
      "are already lost; carry it across the language boundary as a string instead",
  );
}

/**
 * Run `work`, giving up as soon as `signal` aborts.
 *
 * The comm channel cannot cancel a request in flight, so this abandons the reply, not the pull.
 * Nothing else listens for termination while arguments resolve, so a mid-pull kill would stall.
 * `work` is a thunk so an already-aborted task issues no request at all.
 */
async function abortable<T>(work: () => Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw abortError(signal);
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(abortError(signal));
  });
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await Promise.race([work(), aborted]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

function abortError(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return new Error(
    "Aborted while resolving this task's arguments from its upstream tasks: " +
      (reason instanceof Error ? reason.message : String(reason)),
  );
}

/**
 * The object a handler destructures.
 *
 * A Proxy rather than a pre-built object with both spellings, because the SDK
 * has no way to know which spelling a handler will use: it sees Python's names
 * and nothing else. Folding on read means binding needs nothing declared on
 * either side, and no guess about the TypeScript name is ever materialized.
 * It is also what lets a `withArgNames` entry take precedence, decided per read.
 */
function makeArgsProxy(
  names: readonly string[],
  byFold: ReadonlyMap<string, string>,
  values: ReadonlyMap<string, JsonValue>,
  deps: ArgBindingDeps,
): object {
  const { argNames, logs } = deps;
  const resolve = (property: string): string | undefined => {
    // An explicit rename wins and never falls back to folding, so a wrong entry misses.
    const renamed = argNames.get(property);
    if (renamed !== undefined) return values.has(renamed) ? renamed : undefined;
    return values.has(property) ? property : byFold.get(foldArgName(property));
  };

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
        // Tells a wrong `withArgNames` entry apart from an argument the call never passed.
        renamed_to: argNames.get(property) ?? null,
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
