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

import { describe, it, expect } from "vitest";
import {
  Dag,
  finalizeDag,
  getDagTaskInputs,
  getDagTaskRecords,
  type TaskRef,
} from "../../src/sdk/dag.js";
import { Bundle, finalizeBundleDags } from "../../src/sdk/bundle.js";

describe("Dag", () => {
  it("returns a factory whose call yields a frozen TaskRef with the Dag and task identity", () => {
    const dag = new Dag("example_dag");
    const myTask = dag.task("my_task", async () => "hello");
    expect(typeof myTask).toBe("function");

    const ref = myTask();
    expect(ref).toEqual({ dagId: "example_dag", taskId: "my_task" });
    expect(Object.isFrozen(ref)).toBe(true);
  });

  it("chains upstream references into downstream task inputs", () => {
    const dag = new Dag("chained_dag");
    const extract = dag.task("extract", async () => ({ rows: 1 }));
    const transform = dag.task(
      "transform",
      async (_: { extracted: { rows: number } }) => undefined,
    );
    const load = dag.task("load", async (_: { transformed: undefined }) => undefined, { spec: {} });

    const extracted = extract();
    const transformed = transform({ extracted });
    const loaded = load({ transformed });

    expect(extracted).toEqual({ dagId: "chained_dag", taskId: "extract" });
    expect(transformed).toEqual({ dagId: "chained_dag", taskId: "transform" });
    expect(loaded).toEqual({ dagId: "chained_dag", taskId: "load" });

    const inputs = getDagTaskInputs(dag);
    expect(inputs.get("extract")).toEqual({});
    expect(inputs.get("transform")).toEqual({ extracted });
    expect(inputs.get("load")).toEqual({ transformed });
  });

  it("records a literal argument as a value rather than an edge", () => {
    const dag = new Dag("literal_dag");
    const extract = dag.task("extract", async () => 1);
    const transform = dag.task(
      "transform",
      async (_: { extracted: number; regionCode: string; limits: number[] }) => undefined,
    );

    const extracted = extract();
    transform({ extracted, regionCode: "us", limits: [1, 2] });

    expect(getDagTaskInputs(dag).get("transform")).toEqual({
      extracted,
      regionCode: "us",
      limits: [1, 2],
    });
  });

  it("treats an unbranded look-alike reference as a literal, not an edge", () => {
    const dag = new Dag("lookalike_dag");
    const transform = dag.task("transform", async (_: { upstream: unknown }) => undefined);
    const lookalike = { dagId: "lookalike_dag", taskId: "ghost" };

    transform({ upstream: lookalike });

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ upstream: lookalike });
  });

  it("accepts several named inputs for one task", () => {
    const dag = new Dag("fan_in_dag");
    const extract = dag.task("extract", async () => undefined);
    const other = dag.task("other_task", async () => undefined);
    const transform = dag.task(
      "transform",
      async (_: { extracted: undefined; otherTaskResult: undefined }) => undefined,
    );

    const extracted = extract();
    const otherTaskResult = other();
    transform({ extracted, otherTaskResult });

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ extracted, otherTaskResult });
  });

  it("records frozen inputs that later mutation of the caller's object cannot change", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => undefined);
    const transform = dag.task("transform", async (_: { extracted: undefined }) => undefined);

    const extracted = extract();
    const inputs: Record<string, TaskRef> = { extracted };
    transform(inputs as { extracted: TaskRef });

    inputs.sneaky = extracted;
    const recorded = getDagTaskInputs(dag).get("transform")!;
    expect(recorded).toEqual({ extracted });
    expect(Object.isFrozen(recorded)).toBe(true);
  });

  it("rejects an input taken from another Dag", () => {
    const first = new Dag("first_dag");
    const second = new Dag("second_dag");
    const extracted = first.task("extract", async () => undefined)();
    const transform = second.task("transform", async (_: { extracted: TaskRef }) => undefined);

    expect(() => transform({ extracted })).toThrowError(
      /Input "extracted" of task "transform" comes from Dag "first_dag", not "second_dag"/,
    );
  });

  it("rejects a reference from another Dag object carrying the same Dag ID", () => {
    const first = new Dag("same_id");
    const second = new Dag("same_id");
    first.task("extract", async () => undefined);
    const extracted = second.task("extract", async () => undefined)();
    const transform = first.task("transform", async (_: { extracted: TaskRef }) => undefined);

    expect(() => transform({ extracted })).toThrowError(
      /Input "extracted" of task "transform" was not returned by this Dag's "extract"/,
    );
  });

  it("rejects a reference to a task this Dag never registered", () => {
    const first = new Dag("same_id");
    const second = new Dag("same_id");
    const ghost = second.task("ghost", async () => undefined)();
    const transform = first.task("transform", async (_: { ghost: TaskRef }) => undefined);

    expect(() => transform({ ghost })).toThrowError(
      /Input "ghost" of task "transform" refers to unregistered task "ghost"/,
    );
  });

  it("rejects a reference buried inside a literal input", () => {
    // It would draw no edge, so the task would run without the upstream it was
    // given. TypeScript rejects it for a well-typed argument, which leaves the
    // `any`, the cast and the plain-JavaScript caller to this check.
    const dag = new Dag("nested_dag");
    const extract = dag.task("extract", async () => 1);
    const other = dag.task("other", async () => 2);
    const fan = dag.task("fan", async (_: { sources: unknown }) => undefined);
    const sources = [extract(), other()];

    expect(() => fan({ sources } as unknown as { sources: never })).toThrowError(
      /Input "sources" of task "fan" holds a reference to "extract" inside a literal value/,
    );
  });

  it("finds a reference nested several levels down, and survives a self-reference", () => {
    const dag = new Dag("deep_dag");
    const extract = dag.task("extract", async () => 1);
    const fan = dag.task("fan", async (_: { config: unknown }) => undefined);
    const config: Record<string, unknown> = { outer: { inner: [{ from: extract() }] } };
    config["self"] = config;

    expect(() => fan({ config } as unknown as { config: never })).toThrowError(
      /holds a reference to "extract" inside a literal value/,
    );
  });

  it("rejects an input keyed by a symbol", () => {
    const dag = new Dag("symbol_dag");
    const transform = dag.task("transform", async (_: { real: string }) => undefined);
    const inputs = { real: "ok", [Symbol("sneaky")]: "value" };

    expect(() => transform(inputs)).toThrowError(
      /Input "Symbol\(sneaky\)" of task "transform" is keyed by a symbol; an argument name is a string/,
    );
  });

  it("records a positional call in argument order", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<number> => 1);
    const transform = dag.task("transform", async (rows: number, region: string) => `${region}`);

    const extracted = extract();
    transform(extracted, "us");

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ arg0: extracted, arg1: "us" });
    expect(Object.keys(getDagTaskInputs(dag).get("transform")!)).toEqual(["arg0", "arg1"]);
  });

  it("names positional arguments from argNames", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<number> => 1);
    const transform = dag.task("transform", async (rows: number, region: string) => `${region}`, {
      argNames: ["rows", "region"],
    });

    const extracted = extract();
    transform(extracted, "us");

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ rows: extracted, region: "us" });
  });

  it("labels the arguments argNames does not reach", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task("transform", async (rows: number, region: string) => `${region}`, {
      argNames: ["rows"],
    });

    transform(1, "us");

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ rows: 1, arg1: "us" });
  });

  it.each([
    ["not an array", { argNames: 1 }, /argNames for Dag "d" task "t" must be an array of names/],
    ["not a string", { argNames: [1] }, /holds 1; each name must be a non-empty string/],
    ["empty", { argNames: [""] }, /holds ""; each name must be a non-empty string/],
    ["a number", { argNames: ["0"] }, /holds "0"; each name must be a non-empty string/],
    ["a duplicate", { argNames: ["a", "a"] }, /argNames for Dag "d" task "t" names "a" twice/],
  ])("rejects argNames that are %s", (_label, options, expected) => {
    const dag = new Dag("d");

    expect(() => dag.task("t", async (a: number) => a, options as never)).toThrowError(expected);
  });

  it("reads a single argument that is not a map of names as one positional input", () => {
    const dag = new Dag("example_dag");
    const when = new Date();
    const transform = dag.task("transform", async (at: Date) => at);

    transform(when as never);

    expect(getDagTaskInputs(dag).get("transform")).toEqual({ arg0: when });
  });

  it("reads a single reference as one positional input rather than a map of names", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<{ rows: number }> => ({ rows: 1 }));
    const load = dag.task("load", async (totals: { rows: number }) => totals.rows);

    const extracted = extract();
    load(extracted);

    expect(getDagTaskInputs(dag).get("load")).toEqual({ arg0: extracted });
  });

  it("spreads a positional task's bound arguments back into its argument list", async () => {
    const dag = new Dag("example_dag");
    const seen: unknown[] = [];
    const transform = dag.task(
      "transform",
      async (rows: number, region: string) => {
        seen.push(rows, region);
      },
      { argNames: ["rows", "region"] },
    );

    transform(1, "us");
    // The order the runtime hands the bound arguments over in, which is the
    // order they were recorded.
    await new Bundle(dag).getTaskHandler("example_dag", "transform")!({
      rows: 1,
      region: "us",
    } as never);

    expect(seen).toEqual([1, "us"]);
  });

  it("calls a task declaring one object of named arguments with that object", async () => {
    const dag = new Dag("example_dag");
    const seen: unknown[] = [];
    const store = dag.task("store", async ({ rows }: { rows: number }) => {
      seen.push(rows);
    });

    store({ rows: 1 });
    await new Bundle(dag).getTaskHandler("example_dag", "store")!({ rows: 7 } as never);

    expect(seen).toEqual([7]);
  });

  it("rejects calling the same task twice", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async () => undefined);
    extract();

    expect(() => extract()).toThrowError(
      /Task "extract" of Dag "example_dag" was already called; a task holds one place in a Dag/,
    );
  });

  it("fails when the Dag is read with a task that was never called", () => {
    const dag = new Dag("unplaced_dag");
    dag.task("extract", async () => undefined)();
    dag.task("orphan", async () => undefined);

    expect(() => finalizeDag(dag)).toThrowError(
      /Task "orphan" of Dag "unplaced_dag" is never called, so it has no place in the Dag/,
    );
  });

  it("reports the same failure on a second read rather than reporting itself as read", () => {
    const dag = new Dag("unplaced_dag");
    dag.task("orphan", async () => undefined);

    expect(() => finalizeDag(dag)).toThrowError(/is never called/);
    expect(() => finalizeDag(dag)).toThrowError(/is never called/);
  });

  it("surfaces an uncalled task when a bundle reports what it provides", () => {
    const dag = new Dag("served_dag");
    dag.task("orphan", async () => undefined);
    const bundle = new Bundle(dag);

    expect(() => finalizeBundleDags(bundle)).toThrowError(
      /Task "orphan" of Dag "served_dag" is never called/,
    );
  });

  it("rejects a task added after the Dag was read", () => {
    const dag = new Dag("closed_dag");
    dag.task("extract", async () => undefined)();
    finalizeDag(dag);

    expect(() => dag.task("late", async () => undefined)).toThrowError(
      /Task "late" cannot be added to Dag "closed_dag" after the Dag was read/,
    );
  });

  it("rejects wiring through a factory after the Dag was read", () => {
    // A factory outlives the module that built it, so a stray later call has to
    // be rejected rather than silently rewiring a Dag Airflow already read.
    const dag = new Dag("closed_dag");
    const extract = dag.task("extract", async () => undefined);
    extract();
    finalizeDag(dag);

    expect(() => extract()).toThrowError(
      /Task "extract" of Dag "closed_dag" was called after the Dag was read/,
    );
  });

  it("retains its spec and each task's handler and spec, copied and frozen", () => {
    const dagSpec = {};
    const taskSpec = {};
    const handler = async () => "hello";
    const dag = new Dag("example_dag", dagSpec);
    dag.task("my_task", handler, { spec: taskSpec })();

    expect(dag.dagId).toBe("example_dag");
    expect(dag.spec).toEqual(dagSpec);
    expect(Object.isFrozen(dag.spec)).toBe(true);
    const record = getDagTaskRecords(dag).get("my_task");
    expect(record?.fn).toBe(handler);
    expect(record?.spec).toEqual(taskSpec);
    expect(Object.isFrozen(record!.spec)).toBe(true);
  });

  it.each([
    ["a populated object", { schedule: "@daily" }],
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a Dag spec that is not an empty object: %s", (_label, spec) => {
    expect(() => new Dag("example_dag", spec as unknown as Record<string, never>)).toThrowError(
      /spec for Dag "example_dag" must be an empty object/,
    );
  });

  it.each([
    ["a populated object", { retries: 2 }],
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a task spec that is not an empty object: %s", (_label, spec) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, {
        spec: spec as unknown as Record<string, never>,
      }),
    ).toThrowError(/spec for Dag "example_dag" task "transform" must be an empty object/);
    expect(dag.taskIds).toEqual([]);
  });

  it("exposes its task IDs in attachment order", () => {
    const dag = new Dag("ordered_dag");
    expect(dag.taskIds).toEqual([]);
    dag.task("extract", async () => undefined);
    dag.task("transform", async () => undefined);
    expect(dag.taskIds).toEqual(["extract", "transform"]);
  });

  it.each([
    ["inputs, which the factory call now carries", { inputs: {} }],
    ["a misspelled spec key", { specs: {} }],
    ["an upstream reference passed as an option", { upstream: { dagId: "d", taskId: "t" } }],
  ])("rejects %s in the task options", (_label, options) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, options as unknown as Record<string, never>),
    ).toThrowError(/Unknown option ".+" for Dag "example_dag" task "transform"/);
    expect(dag.taskIds).toEqual([]);
  });

  it.each([
    ["null", null],
    ["an array", []],
    ["a string", "spec"],
    ["a non-plain object", new Date()],
  ])("rejects task options that are not an options object: %s", (_label, options) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, options as unknown as Record<string, never>),
    ).toThrowError(/options for Dag "example_dag" task "transform" must be an object/);
  });

  it("rejects duplicate taskIds within a Dag", () => {
    const dag = new Dag("example_dag");
    dag.task("dup", async () => undefined);
    expect(() => dag.task("dup", async () => undefined)).toThrowError(/already registered/);
  });

  it("allows the same taskId in different Dags", () => {
    const first = async () => "first";
    const second = async () => "second";
    const firstDag = new Dag("first_dag");
    const secondDag = new Dag("second_dag");
    firstDag.task("extract", first)();
    secondDag.task("extract", second)();

    const bundle = new Bundle();
    bundle.register(firstDag, secondDag);
    expect(bundle.getTaskHandler("first_dag", "extract")).toBe(first);
    expect(bundle.getTaskHandler("second_dag", "extract")).toBe(second);
  });

  it("accepts a Unicode dagId that Python's word-character rule allows", () => {
    const handler = async () => undefined;
    const dag = new Dag("café_dag");
    dag.task("任務", handler)();
    const bundle = new Bundle();
    bundle.register(dag);
    expect(bundle.getTaskHandler("café_dag", "任務")).toBe(handler);
  });

  it("rejects non-function handlers", () => {
    const dag = new Dag("example_dag");
    expect(() => dag.task("x", "not a function" as unknown as () => Promise<unknown>)).toThrowError(
      /must be a function/,
    );
  });

  it("treats a dotted TaskGroup taskId as a single taskId (group.task)", () => {
    const dag = new Dag("example_dag");
    dag.task("transforms.normalize", async () => "ok")();
    const bundle = new Bundle();
    bundle.register(dag);
    expect(bundle.getTaskHandler("example_dag", "transforms.normalize")).toBeDefined();
    // Should NOT accidentally match the prefix alone
    expect(bundle.getTaskHandler("example_dag", "transforms")).toBeUndefined();
    expect(bundle.getTaskHandler("example_dag", "normalize")).toBeUndefined();
  });
});
