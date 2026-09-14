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

import { bindArgs, foldArgName, type BoundArgs } from "../../src/coordinator/arg-binding.js";
import type { LogChannel } from "../../src/coordinator/log-channel.js";
import type { ArgBindings } from "../../src/generated/supervisor.js";

function literal(name: string, value: unknown, extra: Record<string, unknown> = {}) {
  return { name, kind: "literal" as const, value, ...extra };
}

function makeLogs() {
  const warning = vi.fn();
  const logs = { warning } as unknown as LogChannel;
  return { logs, warning };
}

function bind(bindings: ArgBindings): BoundArgs & { warning: ReturnType<typeof vi.fn> } {
  const { logs, warning } = makeLogs();
  return { ...bindArgs(bindings, logs), warning };
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

describe("bindArgs", () => {
  it("delivers nothing for a task called with no arguments", () => {
    for (const bindings of [null, undefined, []] as (ArgBindings | undefined)[]) {
      const bound = bindArgs(bindings, makeLogs().logs);
      expect(bound.names).toEqual([]);
      expect(Object.keys(bound.args as object)).toEqual([]);
    }
  });

  it("binds a camelCase name to Python's snake_case with nothing declared", () => {
    const { args } = bind([
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

  it("binds in the other direction too, and for a capitalised name", () => {
    // Folding is symmetric, so a Python side that already uses camelCase or a
    // capitalised name needs nothing declared either.
    const { args } = bind([literal("regionCode", "uk"), literal("Name", "United Kingdom")]);
    const { region_code: regionCode, name } = args as { region_code: string; name: string };

    expect({ regionCode, name }).toEqual({ regionCode: "uk", name: "United Kingdom" });
  });

  it("binds an exact name without folding it", () => {
    const { args } = bind([literal("threshold", 0.75)]);
    expect((args as { threshold: number }).threshold).toBe(0.75);
  });

  it("binds every JSON value a literal can carry", () => {
    const { args } = bind([
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

  it("binds an argument the call left at its default", () => {
    // Arrives flagged `from_default`, which changes nothing about the value:
    // the handler cannot tell, and should not need to.
    const { args } = bind([literal("dry_run", true, { from_default: true })]);
    expect((args as { dryRun: boolean }).dryRun).toBe(true);
  });

  it("logs an unmatched name rather than throwing", () => {
    // A destructuring default such as `{ runId = "manual" }` is a legitimate
    // miss, and nothing can tell one from a typo, so a miss cannot fail a task.
    const { args, warning } = bind([literal("region_code", "uk")]);
    const { runId = "manual", reigonCode } = args as { runId?: string; reigonCode?: string };

    expect(runId).toBe("manual");
    expect(reigonCode).toBeUndefined();
    expect(warning).toHaveBeenCalledWith("Task argument not bound by this task's call", {
      requested: "runId",
      bound: ["region_code"],
    });
    // Both the requested name and what the call actually delivered, so a typo
    // is diagnosable from the task log alone.
    expect(warning).toHaveBeenCalledWith("Task argument not bound by this task's call", {
      requested: "reigonCode",
      bound: ["region_code"],
    });
  });

  it("does not log a symbol read as an unbound argument", () => {
    // Promise resolution, string coercion and test frameworks all probe an
    // object with symbols; none of those is an argument that went missing.
    const { args, warning } = bind([literal("region_code", "uk")]);
    void (args as Record<symbol, unknown>)[Symbol.toPrimitive];
    void (args as Record<symbol, unknown>)[Symbol.iterator];

    expect(warning).not.toHaveBeenCalled();
  });

  it("folds `in` like a read", () => {
    const { args } = bind([literal("region_code", "uk")]);

    expect("regionCode" in (args as object)).toBe(true);
    expect("region_code" in (args as object)).toBe(true);
    expect("threshold" in (args as object)).toBe(false);
  });

  it("yields Python's names from Object.keys and rest destructuring", () => {
    // The SDK has no TypeScript-side names to enumerate: it sees the wire's
    // names and nothing else, so that is what enumeration reports.
    const bindings: ArgBindings = [literal("region_code", "uk"), literal("dry_run", false)];
    const { args, names } = bind(bindings);
    const { ...rest } = args as object;

    expect(names).toEqual(["region_code", "dry_run"]);
    expect(Object.keys(args as object)).toEqual(["region_code", "dry_run"]);
    expect(rest).toEqual({ region_code: "uk", dry_run: false });
    expect(Object.entries(args as object)).toEqual([
      ["region_code", "uk"],
      ["dry_run", false],
    ]);
  });

  it("keeps the declaration order of the calling signature", () => {
    const { names } = bind([literal("c", 1), literal("a", 2), literal("b", 3)]);
    expect(names).toEqual(["c", "a", "b"]);
  });

  it("fails at dispatch when two Python names fold to the same token", () => {
    // Neither could be reached by name, and picking either silently would hand
    // the handler the wrong value.
    expect(() => bind([literal("region_code", "uk"), literal("regionCode", "de")])).toThrowError(
      /Task arguments "region_code" and "regionCode" both fold to "regioncode"/,
    );
  });

  it("does not reach Object.prototype for an argument that was not passed", () => {
    // A Python argument named `constructor` or `toString` must bind like any
    // other, and a handler destructuring one that was not passed must miss.
    const { args } = bind([literal("toString", "not a function")]);

    expect((args as { toString: unknown }).toString).toBe("not a function");
    expect((args as { constructor?: unknown }).constructor).toBeUndefined();
    expect("valueOf" in (args as object)).toBe(false);
  });

  it("binds an argument named __proto__ as a key", () => {
    const { args } = bind([literal("__proto__", { polluted: true })]);

    expect(Object.keys(args as object)).toEqual(["__proto__"]);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  it("refuses an XCom-backed argument, naming the upstream task", () => {
    expect(() =>
      bind([{ name: "totals", kind: "xcom" as const, task_id: "make_totals" }]),
    ).toThrowError(/takes the output of upstream task "make_totals"/);
  });

  it("refuses a binding kind from a newer Airflow", () => {
    // Skipping it would leave the argument unbound, and an unbound argument
    // destructures to `undefined` and corrupts the task's output.
    const unknownKind = [{ name: "totals", kind: "dataset" }] as unknown as ArgBindings;
    expect(() => bind(unknownKind)).toThrowError(
      /has binding kind "dataset", which this version of apache-airflow-ts-sdk cannot bind/,
    );
  });

  it("refuses assignment and deletion", () => {
    // The bound object mirrors a call site that already happened, so writing
    // to it would change nothing an author could observe downstream.
    const { args } = bind([literal("region_code", "uk")]);

    expect(() => {
      (args as { regionCode: string }).regionCode = "de";
    }).toThrowError(/bound arguments are read-only/);
    expect(() => {
      delete (args as { region_code?: string }).region_code;
    }).toThrowError(/bound arguments are read-only/);
  });
});
