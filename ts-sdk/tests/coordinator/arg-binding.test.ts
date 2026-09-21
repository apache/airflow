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

import { describe, expect, it, vi } from "vitest";

import { foldArgName, resolveArgs, type BoundArgs } from "../../src/coordinator/arg-binding.js";
import type { CoordinatorClient, XComEntry } from "../../src/coordinator/client.js";
import type { LogChannel } from "../../src/coordinator/log-channel.js";
import type { ArgBindings } from "../../src/generated/supervisor.js";
import type { GetXComOpts } from "../../src/sdk/client-types.js";

function literal(name: string, value: unknown, extra: Record<string, unknown> = {}) {
  return { name, kind: "literal" as const, value, ...extra };
}

function xcom(name: string, taskId: string, extra: Record<string, unknown> = {}) {
  return { name, kind: "xcom" as const, task_id: taskId, ...extra };
}

const NO_RENAMES: ReadonlyMap<string, string> = new Map();

function makeLogs() {
  const warning = vi.fn();
  const logs = { warning } as unknown as LogChannel;
  return { logs, warning };
}

/** An upstream store keyed by task_id, plus the pull calls it saw. */
function makeClient(upstream: Record<string, XComEntry> = {}) {
  const getXComEntry = vi.fn(async (opts: GetXComOpts) => {
    pulls.push(opts);
    return upstream[opts.taskId ?? ""] ?? { found: false, value: null };
  });
  const pulls: GetXComOpts[] = [];
  return { client: { getXComEntry } as unknown as CoordinatorClient, getXComEntry, pulls };
}

interface BindResult extends BoundArgs {
  warning: ReturnType<typeof vi.fn>;
  pulls: GetXComOpts[];
}

async function bind(
  bindings: ArgBindings,
  opts: {
    upstream?: Record<string, XComEntry>;
    signal?: AbortSignal;
    argNames?: Record<string, string>;
  } = {},
): Promise<BindResult> {
  const { logs, warning } = makeLogs();
  const { client, pulls } = makeClient(opts.upstream);
  const signal = opts.signal ?? new AbortController().signal;
  const bound = await resolveArgs(bindings, {
    client,
    signal,
    logs,
    argNames: new Map(Object.entries(opts.argNames ?? {})),
  });
  return { ...bound, warning, pulls };
}

describe("foldArgName", () => {
  it.each([
    ["region_code", "regioncode"],
    ["regionCode", "regioncode"],
    ["Name", "name"],
    ["s3_uri", "s3uri"],
    ["s3Uri", "s3uri"],
    ["dry_run", "dryrun"],
    ["already", "already"],
    ["__dunder__", "dunder"],
  ])("folds %s to %s", (name, folded) => {
    expect(foldArgName(name)).toBe(folded);
  });

  it("matches the Go SDK's rule", () => {
    // strings.ToLower(strings.ReplaceAll(name, "_", "")): only `_` is removed,
    // because it is the only separator a Python parameter name can contain, so
    // one Python signature binds identically in either SDK.
    expect(foldArgName("a-b")).toBe("a-b");
  });
});

describe("resolveArgs", () => {
  it("delivers nothing for a task called with no arguments", async () => {
    for (const bindings of [null, undefined, []] as (ArgBindings | undefined)[]) {
      const bound = await bind(bindings as ArgBindings);
      expect(bound.names).toEqual([]);
      expect(Object.keys(bound.args as object)).toEqual([]);
    }
  });

  it("binds a camelCase name to Python's snake_case with nothing declared", async () => {
    const { args } = await bind([
      literal("region_code", "uk"),
      literal("threshold", 0.75),
      literal("s3_uri", "s3://bucket/key"),
    ]);
    const { regionCode, threshold, s3Uri } = args as {
      regionCode: string;
      threshold: number;
      s3Uri: string;
    };

    expect({ regionCode, threshold, s3Uri }).toEqual({
      regionCode: "uk",
      threshold: 0.75,
      s3Uri: "s3://bucket/key",
    });
  });

  it("binds in the other direction too, and for a capitalised name", async () => {
    // Folding is symmetric, so a Python side that already uses camelCase or a
    // capitalised name needs nothing declared either.
    const { args } = await bind([literal("regionCode", "uk"), literal("Name", "United Kingdom")]);
    const { region_code: regionCode, name } = args as { region_code: string; name: string };

    expect({ regionCode, name }).toEqual({ regionCode: "uk", name: "United Kingdom" });
  });

  it("binds an exact name without folding it", async () => {
    const { args } = await bind([literal("threshold", 0.75)]);
    expect((args as { threshold: number }).threshold).toBe(0.75);
  });

  it("binds every JSON value a literal can carry", async () => {
    const { args } = await bind([
      literal("totals", { orders: 12, revenue: 3402 }),
      literal("regions", ["uk", "de"]),
      literal("dry_run", false),
      literal("retries_used", 3),
      // Airflow omits `value` entirely for a literal whose value is null.
      { name: "label", kind: "literal" as const },
    ]);

    expect({ ...(args as object) }).toEqual({
      totals: { orders: 12, revenue: 3402 },
      regions: ["uk", "de"],
      dry_run: false,
      retries_used: 3,
      label: null,
    });
  });

  it("binds an argument the call left at its default", async () => {
    // Arrives flagged `from_default`, which changes nothing about the value:
    // the handler cannot tell, and should not need to.
    const { args } = await bind([literal("dry_run", true, { from_default: true })]);
    expect((args as { dryRun: boolean }).dryRun).toBe(true);
  });

  it("logs an unmatched name rather than throwing", async () => {
    // A destructuring default such as `{ runId = "manual" }` is a legitimate
    // miss, and nothing can tell one from a typo, so a miss cannot fail a task.
    const { args, warning } = await bind([literal("region_code", "uk")]);
    const { runId = "manual", reigonCode } = args as { runId?: string; reigonCode?: string };

    expect(runId).toBe("manual");
    expect(reigonCode).toBeUndefined();
    expect(warning).toHaveBeenCalledWith("Task argument not bound by this task's call", {
      requested: "runId",
      renamed_to: null,
      bound: ["region_code"],
    });
    // Both the requested name and what the call actually delivered, so a typo
    // is diagnosable from the task log alone.
    expect(warning).toHaveBeenCalledWith("Task argument not bound by this task's call", {
      requested: "reigonCode",
      renamed_to: null,
      bound: ["region_code"],
    });
  });

  it("does not log a symbol read as an unbound argument", async () => {
    // Promise resolution, string coercion and test frameworks all probe an
    // object with symbols; none of those is an argument that went missing.
    const { args, warning } = await bind([literal("region_code", "uk")]);
    void (args as Record<symbol, unknown>)[Symbol.toPrimitive];
    void (args as Record<symbol, unknown>)[Symbol.iterator];

    expect(warning).not.toHaveBeenCalled();
  });

  it("folds `in` like a read", async () => {
    const { args } = await bind([literal("region_code", "uk")]);

    expect("regionCode" in (args as object)).toBe(true);
    expect("region_code" in (args as object)).toBe(true);
    expect("threshold" in (args as object)).toBe(false);
  });

  it("yields Python's names from Object.keys and rest destructuring", async () => {
    // The SDK has no TypeScript-side names to enumerate: it sees the wire's
    // names and nothing else, so that is what enumeration reports.
    const bindings: ArgBindings = [literal("region_code", "uk"), literal("dry_run", false)];
    const { args, names } = await bind(bindings);
    const { ...rest } = args as object;

    expect(names).toEqual(["region_code", "dry_run"]);
    expect(Object.keys(args as object)).toEqual(["region_code", "dry_run"]);
    expect(rest).toEqual({ region_code: "uk", dry_run: false });
    expect(Object.entries(args as object)).toEqual([
      ["region_code", "uk"],
      ["dry_run", false],
    ]);
  });

  it("keeps the declaration order of the calling signature", async () => {
    const { names } = await bind([literal("c", 1), literal("a", 2), literal("b", 3)]);
    expect(names).toEqual(["c", "a", "b"]);
  });

  it("fails at dispatch when two Python names fold to the same token", async () => {
    // Neither could be reached by name, and picking either silently would hand
    // the handler the wrong value.
    await expect(
      bind([literal("region_code", "uk"), literal("regionCode", "de")]),
    ).rejects.toThrowError(
      /Task arguments "region_code" and "regionCode" both fold to "regioncode"/,
    );
  });

  it("does not reach Object.prototype for an argument that was not passed", async () => {
    // A Python argument named `constructor` or `toString` must bind like any
    // other, and a handler destructuring one that was not passed must miss.
    const { args } = await bind([literal("toString", "not a function")]);

    expect((args as { toString: unknown }).toString).toBe("not a function");
    expect((args as { constructor?: unknown }).constructor).toBeUndefined();
    expect("valueOf" in (args as object)).toBe(false);
  });

  it("binds an argument named __proto__ as a key", async () => {
    const { args } = await bind([literal("__proto__", { polluted: true })]);

    expect(Object.keys(args as object)).toEqual(["__proto__"]);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  it("resolves an XCom-backed argument from the upstream task's output", async () => {
    const totals = { orders: 12, revenue: 3402 };
    const { args, pulls } = await bind([xcom("totals", "make_totals")], {
      upstream: { make_totals: { found: true, value: totals } },
    });

    expect((args as { totals: typeof totals }).totals).toEqual(totals);
    // The upstream's return value, under the same key Python `@task` uses.
    expect(pulls).toEqual([{ key: "return_value", taskId: "make_totals" }]);
  });

  it("folds an XCom-backed argument's name like any other", async () => {
    const { args } = await bind([xcom("region_totals", "make_totals")], {
      upstream: { make_totals: { found: true, value: 12 } },
    });
    expect((args as { regionTotals: number }).regionTotals).toBe(12);
  });

  it("binds an upstream that pushed null, rather than failing", async () => {
    // Distinct from an upstream that pushed nothing: `getXCom` answers null for
    // both, which is why binding reads the found flag instead of the value.
    const { args } = await bind([xcom("totals", "make_totals")], {
      upstream: { make_totals: { found: true, value: null } },
    });
    expect((args as { totals: unknown }).totals).toBeNull();
  });

  it("fails when the upstream pushed no output, naming both", async () => {
    // An unbound argument reaches the handler as `undefined` and corrupts its
    // output rather than stopping it.
    await expect(bind([xcom("totals", "make_totals")])).rejects.toThrowError(
      /Task argument "totals" takes the output of upstream task "make_totals", which pushed no return_value XCom/,
    );
  });

  it("pulls every upstream output at once", async () => {
    // A task called with four upstream outputs should wait for one round-trip,
    // not four, so the pulls must all be in flight together.
    let inFlight = 0;
    let peak = 0;
    const client = {
      getXComEntry: async (opts: GetXComOpts): Promise<XComEntry> => {
        inFlight += 1;
        peak = Math.max(peak, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 5));
        inFlight -= 1;
        return { found: true, value: opts.taskId ?? null };
      },
    } as unknown as CoordinatorClient;

    const bound = await resolveArgs(
      [xcom("a", "t_a"), xcom("b", "t_b"), xcom("c", "t_c"), xcom("d", "t_d")],
      { client, signal: new AbortController().signal, logs: makeLogs().logs, argNames: NO_RENAMES },
    );

    expect(peak).toBe(4);
    expect({ ...(bound.args as object) }).toEqual({ a: "t_a", b: "t_b", c: "t_c", d: "t_d" });
  });

  it("mixes literal and upstream arguments in one call", async () => {
    const { args, pulls } = await bind(
      [literal("region_code", "uk"), xcom("totals", "make_totals"), literal("currency", "GBP")],
      { upstream: { make_totals: { found: true, value: { orders: 12 } } } },
    );

    expect({ ...(args as object) }).toEqual({
      region_code: "uk",
      totals: { orders: 12 },
      currency: "GBP",
    });
    // Only the XCom-backed one costs a round-trip.
    expect(pulls).toHaveLength(1);
  });

  it("issues no pull at all when the spec is unhonourable", async () => {
    // Checked in full before anything is resolved, so a bad spec costs no
    // round-trip and leaves no half-resolved call behind.
    const { client, getXComEntry } = makeClient({ make_totals: { found: true, value: 1 } });
    await expect(
      resolveArgs(
        [xcom("totals", "make_totals"), literal("region_code", 1), literal("regionCode", 2)],
        {
          client,
          signal: new AbortController().signal,
          logs: makeLogs().logs,
          argNames: NO_RENAMES,
        },
      ),
    ).rejects.toThrowError(/both fold to "regioncode"/);
    expect(getXComEntry).not.toHaveBeenCalled();
  });

  it("gives up on an already-aborted task without pulling", async () => {
    // Arguments resolve before the handler runs, the one stretch of a task's
    // life with nothing else listening for termination.
    const controller = new AbortController();
    controller.abort(new Error("Task aborted by SIGTERM"));
    const { client, getXComEntry } = makeClient({ make_totals: { found: true, value: 1 } });

    await expect(
      resolveArgs([xcom("totals", "make_totals")], {
        client,
        signal: controller.signal,
        logs: makeLogs().logs,
        argNames: NO_RENAMES,
      }),
    ).rejects.toThrowError(
      /Aborted while resolving this task's arguments.*Task aborted by SIGTERM/,
    );
    expect(getXComEntry).not.toHaveBeenCalled();
  });

  it("stops mid-pull when the task is aborted", async () => {
    const controller = new AbortController();
    const client = {
      getXComEntry: () => new Promise<XComEntry>(() => undefined),
    } as unknown as CoordinatorClient;

    const pending = resolveArgs([xcom("totals", "make_totals")], {
      client,
      signal: controller.signal,
      logs: makeLogs().logs,
      argNames: NO_RENAMES,
    });
    controller.abort(new Error("Task aborted by SIGTERM"));

    await expect(pending).rejects.toThrowError(/Aborted while resolving this task's arguments/);
  });

  it("leaves a literal-only call unaffected by an aborted task", async () => {
    // Nothing is waited on, so there is nothing to give up: the handler still
    // gets its arguments and the abort reaches it through the context signal.
    const controller = new AbortController();
    controller.abort(new Error("Task aborted by SIGTERM"));

    const { args } = await bind([literal("region_code", "uk")], { signal: controller.signal });
    expect((args as { regionCode: string }).regionCode).toBe("uk");
  });

  it.each([
    [
      "a literal",
      literal("count", Number.MAX_SAFE_INTEGER + 2, { value_schema: { format: "int64" } }),
    ],
    ["an upstream output", xcom("count", "make_count", { value_schema: { format: "int64" } })],
  ])("refuses a Python int beyond exact JavaScript range from %s", async (_label, binding) => {
    // It arrives with its low digits already lost, and nothing downstream
    // could notice, so carrying it as a string is the only honest option.
    await expect(
      bind([binding] as ArgBindings, {
        upstream: { make_count: { found: true, value: Number.MAX_SAFE_INTEGER + 2 } },
      }),
    ).rejects.toThrowError(
      /is a 64-bit integer of .*, beyond the .* a JavaScript number holds exactly/,
    );
  });

  it("accepts a Python int JavaScript still holds exactly", async () => {
    const { args } = await bind([
      literal("count", Number.MAX_SAFE_INTEGER, { value_schema: { format: "int64" } }),
    ]);
    expect((args as { count: number }).count).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("refuses a binding kind from a newer Airflow", async () => {
    // Skipping it would leave the argument unbound, and an unbound argument
    // destructures to `undefined` and corrupts the task's output.
    const unknownKind = [{ name: "totals", kind: "dataset" }] as unknown as ArgBindings;
    await expect(bind(unknownKind)).rejects.toThrowError(
      /has binding kind "dataset", which this version of apache-airflow-ts-sdk cannot bind/,
    );
  });

  describe("with explicit renames", () => {
    it("binds a name the Python side never used", async () => {
      const { args } = await bind([literal("run_label", "nightly")], {
        argNames: { label: "run_label" },
      });
      expect((args as { label: string }).label).toBe("nightly");
    });

    it("still folds everything the map does not mention", async () => {
      const { args } = await bind([literal("run_label", "nightly"), literal("region_code", "uk")], {
        argNames: { label: "run_label" },
      });
      const { label, regionCode } = args as { label: string; regionCode: string };
      expect({ label, regionCode }).toEqual({ label: "nightly", regionCode: "uk" });
    });

    it("beats a folded match on the same name", async () => {
      // An author who stated a binding meant it, so the map wins over the
      // name that would otherwise have folded to it.
      const { args } = await bind([literal("run_label", "nightly"), literal("label", "folded")], {
        argNames: { label: "run_label" },
      });
      expect((args as { label: string }).label).toBe("nightly");
    });

    it("leaves the renamed wire name reachable under its own name", async () => {
      // Enumeration reports Python's names, so rest destructuring must keep
      // resolving them whatever the handler renamed.
      const { args } = await bind([literal("run_label", "nightly")], {
        argNames: { label: "run_label" },
      });
      expect({ ...(args as object) }).toEqual({ run_label: "nightly" });
      expect("run_label" in (args as object)).toBe(true);
      expect("label" in (args as object)).toBe(true);
    });

    it("misses rather than falling back when the mapped name was not passed", async () => {
      // Falling back to folding would hand the handler a value the SDK
      // guessed at, and hide the fact that the stated binding was wrong.
      const { args, warning } = await bind([literal("label", "folded")], {
        argNames: { label: "run_label" },
      });

      expect((args as { label?: string }).label).toBeUndefined();
      expect("label" in (args as object)).toBe(false);
      // The wire name the handler asked for, so a wrong entry is diagnosable
      // from the task log rather than looking like an argument never passed.
      expect(warning).toHaveBeenCalledWith("Task argument not bound by this task's call", {
        requested: "label",
        renamed_to: "run_label",
        bound: ["label"],
      });
    });

    it("renames an XCom-backed argument too", async () => {
      const { args } = await bind([xcom("run_totals", "make_totals")], {
        argNames: { totals: "run_totals" },
        upstream: { make_totals: { found: true, value: { orders: 12 } } },
      });
      expect((args as { totals: { orders: number } }).totals).toEqual({ orders: 12 });
    });
  });

  it("refuses assignment and deletion", async () => {
    // The bound object mirrors a call site that already happened, so writing
    // to it would change nothing an author could observe downstream.
    const { args } = await bind([literal("region_code", "uk")]);

    expect(() => {
      (args as { regionCode: string }).regionCode = "de";
    }).toThrowError(/bound arguments are read-only/);
    expect(() => {
      delete (args as { region_code?: string }).region_code;
    }).toThrowError(/bound arguments are read-only/);
  });
});
