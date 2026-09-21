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
  getDagOrderEdges,
  getDagTaskGroups,
  getDagTaskInputs,
  getDagTaskRecords,
  type DagSpec,
  type TaskRef,
  type TaskSpec,
} from "../../src/sdk/dag.js";
import { Bundle, finalizeBundleDags } from "../../src/sdk/bundle.js";
import { withArgList } from "../../src/sdk/arg-list.js";

describe("Dag", () => {
  it("returns a factory whose call yields a frozen TaskRef with the Dag and task identity", () => {
    const dag = new Dag("example_dag");
    const myTask = dag.task("my_task", async () => "hello");
    expect(typeof myTask).toBe("function");

    const ref = myTask();
    expect(ref).toMatchObject({ dagId: "example_dag", taskId: "my_task" });
    expect(Object.isFrozen(ref)).toBe(true);
  });

  it("chains upstream references into downstream task inputs", () => {
    const dag = new Dag("chained_dag");
    const extract = dag.task("extract", async () => ({ rows: 1 }));
    const transform = dag.task(
      "transform",
      async (_: { extracted: { rows: number } }) => undefined,
    );
    const load = dag.task("load", async (_: { transformed: undefined }) => undefined, {});

    const extracted = extract();
    const transformed = transform({ extracted });
    const loaded = load({ transformed });

    expect(extracted).toMatchObject({ dagId: "chained_dag", taskId: "extract" });
    expect(transformed).toMatchObject({ dagId: "chained_dag", taskId: "transform" });
    expect(loaded).toMatchObject({ dagId: "chained_dag", taskId: "load" });

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

    transform({ upstream: lookalike } as unknown as { upstream: TaskRef });

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

  it("records a listed call in the order the handler destructures", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<number> => 1);
    const transform = dag.task(
      "transform",
      async ({ rows, region }: { rows: number; region: string }) => `${region}${rows}`,
    );

    const extracted = extract();
    transform(withArgList(extracted, "us"));

    expect(Object.entries(getDagTaskInputs(dag).get("transform")!)).toEqual([
      ["rows", extracted],
      ["region", "us"],
    ]);
  });

  it("tells one listed order from the other, since position is what binds", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<number> => 1);
    const transform = dag.task(
      "transform",
      async ({ rows, region }: { rows: number; region: string }) => `${region}${rows}`,
    );

    const extracted = extract();
    // Both values are types an argument takes, so which one each supplies is
    // the order it was given in, and nothing else.
    transform(withArgList("us", extracted));

    expect(Object.entries(getDagTaskInputs(dag).get("transform")!)).toEqual([
      ["rows", "us"],
      ["region", extracted],
    ]);
  });

  it("records a listed call and a named call alike", () => {
    const handler = async ({ rows, region }: { rows: number; region: string }) =>
      `${region}${rows}`;
    const listed = new Dag("d");
    listed.task("transform", handler)(withArgList(1, "us"));
    const named = new Dag("d");
    named.task("transform", handler)({ rows: 1, region: "us" });

    expect(getDagTaskInputs(listed).get("transform")).toEqual(
      getDagTaskInputs(named).get("transform"),
    );
  });

  it("calls a listed task with its inputs under the names it recorded", async () => {
    const dag = new Dag("example_dag");
    const seen: unknown[] = [];
    const transform = dag.task(
      "transform",
      async ({ rows, region }: { rows: number; region: string }) => {
        seen.push(rows, region);
      },
    );

    transform(withArgList(1, "us"));
    await new Bundle(dag).getTaskHandler("example_dag", "transform")!({
      rows: 1,
      region: "us",
    } as never);

    expect(seen).toEqual([1, "us"]);
  });

  it("rejects a listed call when the handler's argument is not a plain object pattern", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task(
      "transform",
      async ({ rows, ...rest }: { rows: number; region?: string }) => `${rows}${String(rest)}`,
    );

    expect(() => transform(withArgList(1))).toThrowError(
      /cannot take its inputs in order: its handler's argument is not a plain object pattern/,
    );
  });

  it("rejects a listed call that gives the wrong number of values", () => {
    const dag = new Dag("example_dag");
    const transform = dag.task(
      "transform",
      async ({ rows, region }: { rows: number; region: string }) => `${region}${rows}`,
    );

    expect(() => transform(withArgList(1))).toThrowError(
      /takes 2 arguments \(rows, region\) but was given 1/,
    );
  });

  it.each([
    ["a bare value", 7],
    ["an array of values", [1, "us"]],
  ])("rejects %s where a task's inputs belong", (_label, inputs) => {
    const dag = new Dag("example_dag");
    const load = dag.task("load", async ({ total }: { total: number }) => total);

    expect(() => load(inputs as never)).toThrowError(
      /takes one object naming its inputs — myTask\({ rows, region }\) — or withArgList/,
    );
  });

  it("rejects a bare reference where a task's inputs belong", () => {
    const dag = new Dag("example_dag");
    const extract = dag.task("extract", async (): Promise<{ rows: number }> => ({ rows: 1 }));
    const load = dag.task("load", async ({ totals }: { totals: { rows: number } }) => totals.rows);

    expect(() => load(extract() as never)).toThrowError(/takes one object naming its inputs/);
  });

  it("rejects a handler that declares more than one parameter", () => {
    const dag = new Dag("example_dag");

    expect(() =>
      dag.task("transform", ((rows: number, region: string) => `${region}${rows}`) as never),
    ).toThrowError(/declares 2 parameters; a handler takes one object of named arguments/);
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
    dag.task("my_task", handler, taskSpec)();

    expect(dag.dagId).toBe("example_dag");
    expect(dag.spec).toEqual(dagSpec);
    expect(Object.isFrozen(dag.spec)).toBe(true);
    const record = getDagTaskRecords(dag).get("my_task");
    expect(record?.fn).toBe(handler);
    expect(record?.spec).toEqual(taskSpec);
    expect(Object.isFrozen(record!.spec)).toBe(true);
  });

  it("copies a spec deeply, so editing the array afterwards cannot change what ships", () => {
    // Nothing reads a spec until the Dag is packed, long after the author's
    // module has run, and `tags` is an array they still hold.
    const tags = ["etl"];
    const dag = new Dag("deep_spec_dag", { tags });

    tags.push("injected");

    expect(dag.spec.tags).toEqual(["etl"]);
    expect(Object.isFrozen(dag.spec.tags)).toBe(true);
  });

  it("copies a Date in a spec, so a setter afterwards cannot change what ships", () => {
    const startDate = new Date("2026-01-01T00:00:00Z");
    const dag = new Dag("dated_dag", { startDate });
    const task = dag.task("extract", async () => undefined, { startDate });

    startDate.setFullYear(2030);
    task();

    expect(dag.spec.startDate?.toISOString()).toBe("2026-01-01T00:00:00.000Z");
    expect(getDagTaskRecords(dag).get("extract")?.spec.startDate?.toISOString()).toBe(
      "2026-01-01T00:00:00.000Z",
    );
  });

  it("rejects a spec value that is neither JSON nor a Date", () => {
    expect(() => new Dag("map_dag", { tags: [new Map()] as unknown as string[] })).toThrowError(
      /holds a Map, which cannot be recorded/,
    );
  });

  it("rejects a spec that refers back to itself", () => {
    const spec: Record<string, unknown> = {};
    spec["tags"] = spec;

    expect(() => new Dag("cyclic_dag", spec as never)).toThrowError(
      /The spec for Dag "cyclic_dag" refers back to itself/,
    );
  });

  it("accepts the generated Dag and task fields", () => {
    const dag = new Dag("specced_dag", { schedule: "@daily", tags: ["etl"], catchup: false });
    dag.task("extract", async () => undefined, { retries: 2, retryDelay: 30 })();

    expect(dag.spec).toEqual({ schedule: "@daily", tags: ["etl"], catchup: false });
    expect(getDagTaskRecords(dag).get("extract")?.spec).toEqual({ retries: 2, retryDelay: 30 });
  });

  it.each([
    ["a misspelling", "scheduled"],
    // The generated fields are camelCase, so the schema's own spelling is a
    // typo here rather than a second accepted name.
    ["the raw schema key", "dag_display_name"],
    // Identity is positional, so it is not a spec field.
    ["the positional dag_id", "dagId"],
  ])("rejects %s in the Dag spec", (_label, key) => {
    expect(() => new Dag("example_dag", { [key]: "x" } as unknown as DagSpec)).toThrowError(
      new RegExp(`Unknown option "${key}" in the spec for Dag "example_dag"`),
    );
  });

  it.each([
    ["a misspelling", "retry"],
    ["the raw schema key", "retry_delay"],
  ])("rejects %s in the task spec", (_label, key) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, { [key]: 1 } as unknown as TaskSpec),
    ).toThrowError(
      new RegExp(`Unknown option "${key}" in the spec for Dag "example_dag" task "transform"`),
    );
    expect(dag.taskIds).toEqual([]);
  });

  it.each([
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a Dag spec that is not an options object: %s", (_label, spec) => {
    expect(() => new Dag("example_dag", spec as unknown as DagSpec)).toThrowError(
      /spec for Dag "example_dag" must be an object/,
    );
  });

  it.each([
    ["null", null],
    ["an array", []],
    ["a non-plain object", new Date()],
  ])("rejects a task spec that is not an options object: %s", (_label, spec) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, spec as unknown as TaskSpec),
    ).toThrowError(/spec for Dag "example_dag" task "transform" must be an object/);
    expect(dag.taskIds).toEqual([]);
  });

  describe("an omitted task id", () => {
    it("takes the handler's function name", () => {
      const dag = new Dag("named_dag");
      dag.task(async function extract() {})();

      expect(dag.taskIds).toEqual(["extract"]);
    });

    it("takes the name of a handler declared elsewhere", () => {
      async function transform() {}
      const dag = new Dag("named_dag");
      dag.task(transform)();

      expect(dag.taskIds).toEqual(["transform"]);
    });

    it("still accepts a spec as the second argument", () => {
      const dag = new Dag("named_dag");
      dag.task(async function extract() {}, { retries: 2 })();

      expect(getDagTaskRecords(dag).get("extract")?.spec).toEqual({ retries: 2 });
    });

    it("is taken from the spec when one names the task", () => {
      const dag = new Dag("specced_id_dag");
      dag.task(async function extract() {}, { taskId: "extract_rows" })();

      expect(dag.taskIds).toEqual(["extract_rows"]);
    });

    it("prefers the positional id over the handler name", () => {
      const dag = new Dag("positional_dag");
      dag.task("extract_rows", async function extract() {})();

      expect(dag.taskIds).toEqual(["extract_rows"]);
    });

    it("rejects a positional id and a spec id together", () => {
      // The positional one used to win and the spec's was dropped in silence.
      const dag = new Dag("two_ids_dag");

      expect(() =>
        dag.task("extract_rows", async function extract() {}, { taskId: "from_spec" }),
      ).toThrowError(
        /Task "extract_rows" of Dag "two_ids_dag" also carries taskId "from_spec" in its spec/,
      );
      expect(dag.taskIds).toEqual([]);
    });

    it("fails for an anonymous handler, naming the two ways to give it an id", () => {
      const dag = new Dag("anonymous_dag");

      expect(() => dag.task(async () => 42)).toThrowError(
        /A task of Dag "anonymous_dag" has no id: its handler has no name/,
      );
      expect(dag.taskIds).toEqual([]);
    });

    it("points at the bundler when a handler's name was minified away", () => {
      const dag = new Dag("minified_dag");
      // What a bundle built by something other than airflow-ts-pack can hold:
      // the function is real, but the bundler took its name.
      const minified = Object.defineProperty(async () => 42, "name", { value: "" });

      expect(() => dag.task(minified)).toThrowError(
        /A bundler that drops function names also lands here/,
      );
    });

    it("rejects a handler name Airflow would not accept as an id", () => {
      const dag = new Dag("bound_dag");
      async function extract({ rows }: { rows: number }) {
        return rows;
      }

      // `bind` names the result "bound extract", which has a space in it.
      expect(() => dag.task(extract.bind(null))).toThrowError(
        /would take the id "bound extract" from its handler's name, which Airflow does not accept/,
      );
    });

    it("rejects a non-function where a handler belongs", () => {
      const dag = new Dag("bad_handler_dag");

      expect(() => dag.task("x", 42 as unknown as () => Promise<void>)).toThrowError(
        /handler for Dag "bad_handler_dag" task "x" must be a function/,
      );
    });
  });

  describe("order-only edges", () => {
    /** A Dag whose tasks are all placed, ready for edges to be drawn on it. */
    function placedDag(dagId: string, ...taskIds: string[]) {
      const dag = new Dag(dagId);
      const refs = Object.fromEntries(
        taskIds.map((taskId) => [taskId, dag.task(taskId, async () => undefined)()]),
      );
      return { dag, refs };
    }

    it("draws an edge with before, from the receiver to the argument", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");

      refs.load!.before(refs.cleanup!);

      expect(getDagOrderEdges(dag)).toEqual([{ upstream: "load", downstream: "cleanup" }]);
    });

    it("draws an edge with after, from the argument to the receiver", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");

      refs.cleanup!.after(refs.load!);

      expect(getDagOrderEdges(dag)).toEqual([{ upstream: "load", downstream: "cleanup" }]);
    });

    it("fans out from one before call", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup", "notify");

      refs.load!.before(refs.cleanup!, refs.notify!);

      expect(getDagOrderEdges(dag)).toEqual([
        { upstream: "load", downstream: "cleanup" },
        { upstream: "load", downstream: "notify" },
      ]);
    });

    it("fans in from one after call", () => {
      const { dag, refs } = placedDag("d", "load", "transform", "cleanup");

      refs.cleanup!.after(refs.load!, refs.transform!);

      expect(getDagOrderEdges(dag)).toEqual([
        { upstream: "load", downstream: "cleanup" },
        { upstream: "transform", downstream: "cleanup" },
      ]);
    });

    it.each([
      ["before", (a: TaskRef, b: TaskRef) => a.before(b)],
      ["after", (a: TaskRef, b: TaskRef) => b.after(a)],
    ])("returns the receiver from %s, not the arguments", (_verb, draw) => {
      const { refs } = placedDag("d", "load", "cleanup");

      // A fan-out has no single "next" reference, so chaining continues from
      // the same task rather than from what was just pointed at.
      expect(draw(refs.load!, refs.cleanup!)).toBe(_verb === "before" ? refs.load : refs.cleanup);
    });

    it("records an edge once however many times it is drawn", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");

      refs.load!.before(refs.cleanup!);
      refs.load!.before(refs.cleanup!);
      refs.cleanup!.after(refs.load!);

      expect(getDagOrderEdges(dag)).toEqual([{ upstream: "load", downstream: "cleanup" }]);
    });

    it("rejects an edge from a task to itself", () => {
      const { refs } = placedDag("d", "a");

      expect(() => refs.a!.before(refs.a!)).toThrowError(
        /before\(\) cannot draw an edge from node "a" of Dag "d" to itself/,
      );
      expect(() => refs.a!.after(refs.a!)).toThrowError(
        /after\(\) cannot draw an edge from node "a" of Dag "d" to itself/,
      );
    });

    it("keeps the two directions apart", () => {
      const { dag, refs } = placedDag("d", "a", "b");

      refs.a!.before(refs.b!);
      refs.b!.before(refs.a!);

      // Two distinct edges, both recorded: rejecting the cycle they form is a
      // Dag-level concern, not an edge-level one.
      expect(getDagOrderEdges(dag)).toEqual([
        { upstream: "a", downstream: "b" },
        { upstream: "b", downstream: "a" },
      ]);
    });

    it("leaves a frozen edge that a caller cannot rewrite", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");
      refs.load!.before(refs.cleanup!);

      expect(Object.isFrozen(getDagOrderEdges(dag)[0])).toBe(true);
    });

    it("carries no value, so it records no input", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");

      refs.load!.before(refs.cleanup!);

      expect(getDagTaskInputs(dag).get("cleanup")).toEqual({});
    });

    it.each([
      ["before", (ref: TaskRef, other: TaskRef) => ref.before(other)],
      ["after", (ref: TaskRef, other: TaskRef) => ref.after(other)],
    ])("rejects a %s edge to a task of another Dag", (verb, draw) => {
      const { refs: here } = placedDag("here", "load");
      const { refs: there } = placedDag("there", "cleanup");

      expect(() => draw(here.load!, there.cleanup!)).toThrowError(
        new RegExp(`${verb}\\(\\) cannot reach Dag "there" node "cleanup" from Dag "here"`),
      );
    });

    it("rejects a reference from another Dag object carrying the same Dag ID", () => {
      const { refs: first } = placedDag("same_id", "load");
      const { refs: second } = placedDag("same_id", "cleanup");

      expect(() => first.load!.before(second.cleanup!)).toThrowError(
        /before\(\) was given a reference to "cleanup" that this Dag did not hand out/,
      );
    });

    it.each([
      ["a plain object", { dagId: "d", taskId: "cleanup" }],
      ["a string", "cleanup"],
      ["null", null],
    ])("rejects %s where a reference belongs", (_label, value) => {
      const { refs } = placedDag("d", "load");

      expect(() => refs.load!.before(value as unknown as TaskRef)).toThrowError(
        /before\(\) on Dag "d" takes tasks and task groups this Dag handed out/,
      );
    });

    it("records nothing when an edge is rejected", () => {
      const { dag, refs } = placedDag("d", "load");

      expect(() => refs.load!.before("cleanup" as unknown as TaskRef)).toThrow();
      expect(getDagOrderEdges(dag)).toEqual([]);
    });

    it("rejects an edge drawn after the Dag was read", () => {
      const { dag, refs } = placedDag("d", "load", "cleanup");
      finalizeDag(dag);

      expect(() => refs.load!.before(refs.cleanup!)).toThrowError(
        /An edge was drawn on Dag "d" after the Dag was read/,
      );
    });

    it("has no edges before any are drawn", () => {
      const { dag } = placedDag("d", "load");
      expect(getDagOrderEdges(dag)).toEqual([]);
    });
  });

  describe("task groups", () => {
    it("prefixes the id of every task declared in it", () => {
      const dag = new Dag("grouped");
      const staging = dag.taskGroup("staging");
      staging.task("stage_rows", async () => undefined)();

      expect(dag.taskIds).toEqual(["staging.stage_rows"]);
    });

    it("prefixes a task id defaulted from the handler name", () => {
      const dag = new Dag("grouped");
      dag.taskGroup("staging").task(async function stageRows() {})();

      expect(dag.taskIds).toEqual(["staging.stageRows"]);
    });

    it("nests, joining every enclosing group's id", () => {
      const dag = new Dag("grouped");
      const outer = dag.taskGroup("outer");
      const inner = outer.taskGroup("inner");
      inner.task("deep", async () => undefined)();

      expect(dag.taskIds).toEqual(["outer.inner.deep"]);
      expect(inner.groupId).toBe("outer.inner");
    });

    it("carries the Dag's own identity", () => {
      const dag = new Dag("grouped");
      expect(dag.taskGroup("staging").dagId).toBe("grouped");
    });

    it("records the tree, so nested groups serialize as one", () => {
      const dag = new Dag("grouped");
      const outer = dag.taskGroup("outer");
      outer.task("first", async () => undefined)();
      const inner = outer.taskGroup("inner");
      inner.task("second", async () => undefined)();

      expect([...getDagTaskGroups(dag).values()]).toEqual([
        {
          groupId: "outer",
          taskIds: ["outer.first"],
          childGroupIds: ["outer.inner"],
        },
        {
          groupId: "outer.inner",
          parentGroupId: "outer",
          taskIds: ["outer.inner.second"],
          childGroupIds: [],
        },
      ]);
    });

    it("lets the same handler name be reused across groups", () => {
      const dag = new Dag("grouped");
      dag.taskGroup("north").task(async function extract() {})();
      dag.taskGroup("south").task(async function extract() {})();

      expect(dag.taskIds).toEqual(["north.extract", "south.extract"]);
    });

    it("lets a task in a group be wired to one outside it", () => {
      const dag = new Dag("grouped");
      const extracted = dag.task("extract", async () => 1)();
      const staged = dag
        .taskGroup("staging")
        .task(
          "stage",
          async (_: { extracted: number }) => undefined,
        )({ extracted });

      expect(staged.taskId).toBe("staging.stage");
      expect(getDagTaskInputs(dag).get("staging.stage")).toEqual({ extracted });
    });

    describe("as an edge endpoint", () => {
      it("orders a whole group before a task", () => {
        const dag = new Dag("grouped");
        const staging = dag.taskGroup("staging");
        staging.task("stage", async () => undefined)();
        const loaded = dag.task("load", async () => undefined)();

        staging.before(loaded);

        expect(getDagOrderEdges(dag)).toEqual([{ upstream: "staging", downstream: "load" }]);
      });

      it("orders a task before a whole group", () => {
        const dag = new Dag("grouped");
        const extracted = dag.task("extract", async () => undefined)();
        const staging = dag.taskGroup("staging");
        staging.task("stage", async () => undefined)();

        extracted.before(staging);

        expect(getDagOrderEdges(dag)).toEqual([{ upstream: "extract", downstream: "staging" }]);
      });

      it("orders one group against another", () => {
        const dag = new Dag("grouped");
        const first = dag.taskGroup("first");
        first.task("a", async () => undefined)();
        const second = dag.taskGroup("second");
        second.task("b", async () => undefined)();

        second.after(first);

        expect(getDagOrderEdges(dag)).toEqual([{ upstream: "first", downstream: "second" }]);
      });

      it("returns its own receiver, as a task does", () => {
        const dag = new Dag("grouped");
        const staging = dag.taskGroup("staging");
        staging.task("stage", async () => undefined)();
        const loaded = dag.task("load", async () => undefined)();

        expect(staging.before(loaded)).toBe(staging);
      });

      it("rejects a group from another Dag object with the same ID", () => {
        const first = new Dag("same_id");
        const second = new Dag("same_id");
        const loaded = first.task("load", async () => undefined)();
        const foreign = second.taskGroup("staging");

        expect(() => loaded.before(foreign)).toThrowError(
          /before\(\) was given a reference to "staging" that this Dag did not hand out/,
        );
      });

      it("rejects a foreign group even when this Dag has one of the same ID", () => {
        // Matching on the ID alone used to accept it, and the edge was then
        // drawn at this Dag's own group of that name.
        const first = new Dag("same_id");
        const second = new Dag("same_id");
        first.taskGroup("staging").task("stage", async () => undefined)();
        const loaded = first.task("load", async () => undefined)();
        const foreign = second.taskGroup("staging");

        expect(() => loaded.before(foreign)).toThrowError(
          /before\(\) was given a reference to "staging" that this Dag did not hand out/,
        );
        expect(getDagOrderEdges(first)).toEqual([]);
      });
    });

    it.each([
      ["an empty ID", ""],
      ["a non-string ID", 42],
    ])("rejects %s", (_label, groupId) => {
      const dag = new Dag("grouped");
      expect(() => dag.taskGroup(groupId as string)).toThrowError(
        /A task group of Dag "grouped" must have a non-empty ID/,
      );
    });

    it("rejects a group ID holding the separator, which nesting is for", () => {
      const dag = new Dag("grouped");
      expect(() => dag.taskGroup("outer.inner")).toThrowError(
        /Task group ID "outer.inner" of Dag "grouped" cannot contain "\."; nest groups with taskGroup/,
      );
    });

    it.each([
      [
        "a task and a group",
        (dag: Dag) => [() => dag.task("x", async () => undefined), () => dag.taskGroup("x")],
      ],
      [
        "a group and a task",
        (dag: Dag) => [() => dag.taskGroup("x"), () => dag.task("x", async () => undefined)],
      ],
      ["two groups", (dag: Dag) => [() => dag.taskGroup("x"), () => dag.taskGroup("x")]],
    ])(
      "rejects %s sharing one ID, since a serialized Dag addresses both by it",
      (_label, build) => {
        const dag = new Dag("grouped");
        const [first, second] = build(dag);

        first!();
        expect(() => second!()).toThrowError(/"x" is already registered for Dag "grouped"/);
      },
    );

    it("allows the same group ID under different parents", () => {
      const dag = new Dag("grouped");
      dag
        .taskGroup("north")
        .taskGroup("shared")
        .task("t", async () => undefined)();
      dag
        .taskGroup("south")
        .taskGroup("shared")
        .task("t", async () => undefined)();

      expect(dag.taskIds).toEqual(["north.shared.t", "south.shared.t"]);
    });

    it("rejects a group declared after the Dag was read", () => {
      const dag = new Dag("grouped");
      dag.task("extract", async () => undefined)();
      finalizeDag(dag);

      expect(() => dag.taskGroup("late")).toThrowError(
        /Task group "late" cannot be added to Dag "grouped" after the Dag was read/,
      );
    });

    it("holds its tasks to the same every-task-is-called rule", () => {
      const dag = new Dag("grouped");
      dag.taskGroup("staging").task("stage", async () => undefined);

      expect(() => finalizeDag(dag)).toThrowError(
        /Task "staging.stage" of Dag "grouped" is never called/,
      );
    });

    it("has no groups before any are declared", () => {
      expect(getDagTaskGroups(new Dag("plain")).size).toBe(0);
    });
  });

  describe("cycle detection", () => {
    it("rejects a cycle drawn with before and after, naming the tasks on it", () => {
      const dag = new Dag("cyclic");
      const a = dag.task("a", async () => undefined)();
      const b = dag.task("b", async () => undefined)();
      a.before(b);
      b.before(a);

      expect(() => finalizeDag(dag)).toThrowError(/Dag "cyclic" has a cycle: a >> b >> a/);
    });

    it("rejects a cycle that runs through both a wired and an order-only edge", () => {
      // Neither kind forms one alone: the wiring is written first and cannot
      // point backwards, and there is a single order-only edge.
      const dag = new Dag("mixed_cycle");
      const extract = dag.task("extract", async () => 1);
      const transform = dag.task("transform", async (_: { extracted: number }) => undefined);
      const extracted = extract();
      const transformed = transform({ extracted });
      extracted.after(transformed);

      expect(() => finalizeDag(dag)).toThrowError(
        /Dag "mixed_cycle" has a cycle: extract >> transform >> extract/,
      );
    });

    it("explains that an edge runs in one direction", () => {
      const dag = new Dag("cyclic");
      const a = dag.task("a", async () => undefined)();
      const b = dag.task("b", async () => undefined)();
      a.before(b).after(b);

      expect(() => finalizeDag(dag)).toThrowError(
        /a Dag cannot come back to a task it has already run/,
      );
    });

    it("surfaces the cycle when a bundle reports what it provides", () => {
      const dag = new Dag("cyclic");
      const a = dag.task("a", async () => undefined)();
      const b = dag.task("b", async () => undefined)();
      a.before(b);
      b.before(a);

      expect(() => finalizeBundleDags(new Bundle(dag))).toThrowError(/has a cycle/);
    });

    it("reports the same cycle on a second read", () => {
      const dag = new Dag("cyclic");
      const a = dag.task("a", async () => undefined)();
      const b = dag.task("b", async () => undefined)();
      a.before(b);
      b.before(a);

      expect(() => finalizeDag(dag)).toThrowError(/has a cycle/);
      expect(() => finalizeDag(dag)).toThrowError(/has a cycle/);
    });

    it("reports an uncalled task before looking for a cycle", () => {
      // The simpler fault first: a task with no place in the Dag has no edges
      // to be on a cycle with.
      const dag = new Dag("both_wrong");
      const a = dag.task("a", async () => undefined)();
      const b = dag.task("b", async () => undefined)();
      dag.task("orphan", async () => undefined);
      a.before(b);
      b.before(a);

      expect(() => finalizeDag(dag)).toThrowError(/Task "orphan" .* is never called/);
    });

    describe("through groups", () => {
      it("catches a cycle formed only by group edges", () => {
        const dag = new Dag("group_cycle");
        const first = dag.taskGroup("first");
        first.task("a", async () => undefined)();
        const second = dag.taskGroup("second");
        second.task("b", async () => undefined)();

        first.before(second);
        second.before(first);

        expect(() => finalizeDag(dag)).toThrowError(
          /Dag "group_cycle" has a cycle: first\.a >> second\.b >> first\.a/,
        );
      });

      it("catches a cycle between a group and a task outside it", () => {
        const dag = new Dag("group_task_cycle");
        const staging = dag.taskGroup("staging");
        staging.task("stage", async () => undefined)();
        const loaded = dag.task("load", async () => undefined)();

        staging.before(loaded);
        loaded.before(staging);

        expect(() => finalizeDag(dag)).toThrowError(
          /has a cycle: staging\.stage >> load >> staging\.stage/,
        );
      });

      it("sees a task held by a nested group", () => {
        const dag = new Dag("nested_cycle");
        const outer = dag.taskGroup("outer");
        outer.taskGroup("inner").task("deep", async () => undefined)();
        const loaded = dag.task("load", async () => undefined)();

        outer.before(loaded);
        loaded.before(outer);

        expect(() => finalizeDag(dag)).toThrowError(
          /has a cycle: outer\.inner\.deep >> load >> outer\.inner\.deep/,
        );
      });

      it("accepts an acyclic Dag whose groups carry edges", () => {
        const dag = new Dag("acyclic_groups");
        const staging = dag.taskGroup("staging");
        const staged = staging.task("stage", async () => undefined)();
        const checked = staging.task("check", async () => undefined)();
        staged.before(checked);
        const loaded = dag.task("load", async () => undefined)();
        const notified = dag.task("notify", async () => undefined)();

        // Both group members feed load, and both are already ordered against
        // each other; a group edge must not be read as making that a cycle.
        staging.before(loaded);
        loaded.before(notified);

        expect(() => finalizeDag(dag)).not.toThrow();
      });

      it("accepts an edge to an empty group, which stands for no task", () => {
        const dag = new Dag("empty_group");
        const loaded = dag.task("load", async () => undefined)();
        const empty = dag.taskGroup("empty");

        loaded.before(empty);
        empty.before(loaded);

        expect(() => finalizeDag(dag)).not.toThrow();
      });
    });
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
    ["a nested spec, left over from the old options object", { spec: {} }],
    ["an upstream reference", { upstream: { dagId: "d", taskId: "t" } }],
  ])("rejects %s in the task spec", (_label, spec) => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, spec as unknown as TaskSpec),
    ).toThrowError(/Unknown option ".+" in the spec for Dag "example_dag" task "transform"/);
    expect(dag.taskIds).toEqual([]);
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

  it("rejects a dotted task id, which names a group that does not exist", () => {
    // A dotted id used to be accepted verbatim, giving a task whose id carries
    // a group prefix while the Dag holds no such group. `taskGroup(...)` is
    // what puts a task under a prefix now.
    const dag = new Dag("example_dag");

    expect(() => dag.task("transforms.normalize", async () => "ok")).toThrowError(
      /Task ID "transforms.normalize" of Dag "example_dag" cannot contain "\."/,
    );
    expect(dag.taskIds).toEqual([]);
  });

  it("names a task inside a group without the author writing the prefix", () => {
    const dag = new Dag("example_dag");
    dag.taskGroup("transforms").task("normalize", async () => "ok")();
    const bundle = new Bundle(dag);

    expect(bundle.getTaskHandler("example_dag", "transforms.normalize")).toBeDefined();
    // And not the prefix or the leaf on its own.
    expect(bundle.getTaskHandler("example_dag", "transforms")).toBeUndefined();
    expect(bundle.getTaskHandler("example_dag", "normalize")).toBeUndefined();
  });
});
