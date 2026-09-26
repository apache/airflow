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

import { describe, expect, it } from "vitest";

import { withArgList } from "../../src/sdk/arg-list.js";
import {
  computeRelativeFileloc,
  serializeDag,
  serializeValue,
  unwrapTypeEncoding,
} from "../../src/coordinator/serde.js";
import {
  Dag,
  type DagSpec,
  type TaskGroupRef,
  type TaskRef,
  type TaskSpec,
} from "../../src/sdk/dag.js";

type Json = Record<string, unknown>;
type WiredArgs = Record<string, TaskRef>;

/** Declare a task and place it, optionally behind some upstreams. */
function place(
  scope: Dag | TaskGroupRef,
  taskId: string,
  upstream: readonly TaskRef[] = [],
  spec?: TaskSpec,
) {
  const factory = scope.task(taskId, async (_args: WiredArgs) => undefined, spec);
  const inputs: Record<string, TaskRef> = {};
  upstream.forEach((ref, index) => {
    inputs[`in_${index}`] = ref;
  });
  return factory(inputs);
}

/** A one-task Dag, serialized. */
function serializeWith(spec: DagSpec, taskSpec?: TaskSpec): Json {
  const dag = new Dag("d", spec);
  place(dag, "t", [], taskSpec);
  return serializeDag(dag, "/bundles/app/bundle.mjs", "bundle.mjs") as Json;
}

function taskVar(serialized: Json, index = 0): Json {
  const tasks = serialized["tasks"] as { __type: string; __var: Json }[];
  expect(tasks[index]!.__type).toBe("operator");
  return tasks[index]!.__var;
}

/** Serialized tasks keyed by task id. */
function taskMap(serialized: Json): Map<string, Json> {
  const tasks = serialized["tasks"] as { __var: Json }[];
  return new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
}

describe("serializeDag", () => {
  it("writes the fields Airflow always expects", () => {
    const serialized = serializeWith({});

    expect(serialized).toMatchObject({
      dag_id: "d",
      fileloc: "/bundles/app/bundle.mjs",
      relative_fileloc: "bundle.mjs",
      timezone: "UTC",
      dag_dependencies: [],
      edge_info: {},
      params: [],
      deadline: null,
      allowed_run_types: null,
      max_active_tasks: 16,
      max_active_runs: 16,
      max_consecutive_failed_dag_runs: 0,
      catchup: false,
      disable_bundle_versioning: false,
    });
  });

  it("puts every task in the flat root group", () => {
    const dag = new Dag("d");
    const first = place(dag, "first");
    place(dag, "second", [first]);

    expect(serializeDag(dag, "", ".")["task_group"]).toEqual({
      _group_id: null,
      group_display_name: "",
      prefix_group_id: true,
      tooltip: "",
      ui_color: "CornflowerBlue",
      ui_fgcolor: "#000",
      children: { first: ["operator", "first"], second: ["operator", "second"] },
      upstream_group_ids: [],
      downstream_group_ids: [],
      upstream_task_ids: [],
      downstream_task_ids: [],
    });
  });

  it("identifies a task as TypeScript rather than as a Python operator", () => {
    expect(taskVar(serializeWith({}))).toEqual({
      task_id: "t",
      task_type: "TypeScriptOperator",
      _task_module: "airflow.sdk.coordinators.node",
      language: "typescript",
      template_fields: [],
      is_stub: true,
    });
  });

  describe("queue", () => {
    it("gives every task the Dag's queue, which is what routes them here", () => {
      const dag = new Dag("d", { queue: "typescript" });
      place(dag, "one");
      place(dag, "two");

      const tasks = taskMap(serializeDag(dag, "", ".") as Json);
      expect([...tasks.values()].map((task) => task["queue"])).toEqual([
        "typescript",
        "typescript",
      ]);
    });

    it("lets a task name its own queue instead", () => {
      const dag = new Dag("d", { queue: "typescript" });
      place(dag, "light");
      place(dag, "heavy", [], { queue: "typescript_large" });

      const tasks = taskMap(serializeDag(dag, "", ".") as Json);
      expect(tasks.get("light")?.["queue"]).toBe("typescript");
      expect(tasks.get("heavy")?.["queue"]).toBe("typescript_large");
    });

    it("writes no queue when neither the Dag nor the task names one", () => {
      expect(taskVar(serializeWith({}))).not.toHaveProperty("queue");
    });

    it("omits a queue that is already the schema default", () => {
      // The scheduler re-derives it, as it does any other defaulted field.
      const dag = new Dag("d", { queue: "default" });
      place(dag, "one");

      expect(taskMap(serializeDag(dag, "", ".") as Json).get("one")).not.toHaveProperty("queue");
    });

    it("is not written onto the Dag itself, which has no queue field", () => {
      expect(serializeWith({ queue: "typescript" })).not.toHaveProperty("queue");
    });
  });

  describe("arg bindings", () => {
    it("binds an upstream reference as the xcom the API server resolves", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      const transform = dag.task("transform", async (_: { extracted: unknown }) => undefined);
      transform({ extracted });

      expect(taskMap(serializeDag(dag, "", ".") as Json).get("transform")).toMatchObject({
        is_stub: true,
        _arg_bindings: [{ name: "extracted", kind: "xcom", task_id: "extract" }],
      });
    });

    it("binds a literal as the value itself", () => {
      const dag = new Dag("d");
      const transform = dag.task(
        "transform",
        async (_: { regionCode: string; limits: number[] }) => undefined,
      );
      transform({ regionCode: "us", limits: [1, 2] });

      expect(taskMap(serializeDag(dag, "", ".") as Json).get("transform")).toMatchObject({
        _arg_bindings: [
          { name: "regionCode", kind: "literal", value: "us" },
          { name: "limits", kind: "literal", value: [1, 2] },
        ],
      });
    });

    it("keeps the order the call listed the arguments in", () => {
      const dag = new Dag("d");
      const north = place(dag, "north");
      const south = place(dag, "south");
      const summarize = dag.task(
        "summarize",
        async (_: { south: unknown; north: unknown; label: string }) => undefined,
      );
      summarize({ south, north, label: "both" });

      const bindings = taskMap(serializeDag(dag, "", ".") as Json).get("summarize")![
        "_arg_bindings"
      ] as { name: string }[];
      expect(bindings.map(({ name }) => name)).toEqual(["south", "north", "label"]);
    });

    it("binds a listed call under the keys its handler destructures", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      // Read off the handler's argument pattern at the call, so a listed call
      // serializes as the same bindings a named one would.
      const transform = dag.task(
        "transform",
        async ({ rows, region }: { rows: number; region: string }) => `${region}${rows}`,
      );
      transform(withArgList(extracted, "us"));

      expect(taskMap(serializeDag(dag, "", ".") as Json).get("transform")).toMatchObject({
        _arg_bindings: [
          { name: "rows", kind: "xcom", task_id: "extract" },
          { name: "region", kind: "literal", value: "us" },
        ],
      });
    });

    it("leaves the key out for a task called with no arguments", () => {
      expect(taskVar(serializeWith({}))).not.toHaveProperty("_arg_bindings");
    });
  });

  describe("timetable", () => {
    it.each([
      ["unset", undefined, { __type: "airflow.timetables.simple.NullTimetable", __var: {} }],
      ["@once", "@once", { __type: "airflow.timetables.simple.OnceTimetable", __var: {} }],
      [
        "@continuous",
        "@continuous",
        { __type: "airflow.timetables.simple.ContinuousTimetable", __var: {} },
      ],
      [
        "a cron expression",
        "0 3 * * *",
        {
          __type: "airflow.timetables.trigger.CronTriggerTimetable",
          __var: {
            expression: "0 3 * * *",
            timezone: "UTC",
            interval: 0,
            run_immediately: false,
          },
        },
      ],
    ])("maps %s onto the matching timetable", (_name, schedule, expected) => {
      expect(serializeWith({ schedule })["timetable"]).toEqual(expected);
    });

    it.each([
      ["@hourly", "0 * * * *"],
      ["@daily", "0 0 * * *"],
      ["@weekly", "0 0 * * 0"],
      ["@monthly", "0 0 1 * *"],
      ["@quarterly", "0 0 1 */3 *"],
      ["@yearly", "0 0 1 1 *"],
    ])("expands the preset %s the way Python records it", (schedule, expression) => {
      // Both spellings rebuild the same timetable, but the expression is what
      // the Dag's summary and its hash are taken from, so writing the preset
      // verbatim would disagree with the same Dag written in Python.
      const timetable = serializeWith({ schedule })["timetable"] as Json;
      expect(timetable["__type"]).toBe("airflow.timetables.trigger.CronTriggerTimetable");
      expect((timetable["__var"] as Json)["expression"]).toBe(expression);
    });

    it.each([
      ["prose", "every tuesday"],
      ["too few fields", "0 0 * *"],
      ["too many fields", "0 0 * * * * *"],
      ["an unknown preset", "@fortnightly"],
    ])("rejects %s rather than writing a schedule nothing can parse", (_label, schedule) => {
      expect(() => serializeWith({ schedule })).toThrowError(
        /is not a cron expression or a preset/,
      );
    });

    it.each([
      ["a six-field expression", "0 0 0 * * *"],
      ["named weekdays", "0 0 * * MON-FRI"],
      ["a step", "*/15 * * * *"],
    ])("accepts %s", (_label, schedule) => {
      expect(() => serializeWith({ schedule })).not.toThrow();
    });

    it.each([
      ["an asset expression", { assets: ["s3://bucket/key"] }, /an object schedule names a Python/],
      ["a number", 86400, /a number schedule names a Python/],
      ["an empty string", "", /schedule for Dag "d" is empty/],
      ["a blank string", "   ", /schedule for Dag "d" is empty/],
    ])("rejects %s", (_name, schedule, expected) => {
      expect(() => serializeWith({ schedule } as DagSpec)).toThrowError(expected);
    });
  });

  describe("non-decorated fields", () => {
    it("writes them as bare values, without the type encoding", () => {
      const serialized = serializeWith({
        startDate: new Date("2026-01-01T00:00:00Z"),
        endDate: new Date("2026-12-31T23:30:15Z"),
        dagrunTimeout: 300,
        tags: ["gamma", "alpha"],
        description: "demo",
      });

      expect(serialized).toMatchObject({
        start_date: 1767225600,
        end_date: 1798759815,
        dagrun_timeout: 300,
        // Python holds tags in a set and writes them sorted.
        tags: ["alpha", "gamma"],
        description: "demo",
      });
    });

    it("keeps only what a decorated field would keep", () => {
      // The two halves of the split: everything above went through
      // serializeValue and then lost its wrapper, because no authoring field is
      // in Python's decorated set. A decorated field would stop at the first.
      const wrapped = serializeValue(new Date("2026-01-01T00:00:00Z"));
      expect(wrapped).toEqual({ __type: "datetime", __var: 1767225600 });
      expect(unwrapTypeEncoding(wrapped)).toBe(1767225600);
    });

    it("collapses duplicate tags, as the set Python holds them in does", () => {
      expect(serializeWith({ tags: ["b", "a", "b"] })["tags"]).toEqual(["a", "b"]);
    });
  });

  describe("omit-if-default", () => {
    it("omits a Dag field left at its schema default", () => {
      const serialized = serializeWith({ failFast: false, renderTemplateAsNativeObj: false });
      expect(serialized).not.toHaveProperty("fail_fast");
      expect(serialized).not.toHaveProperty("render_template_as_native_obj");
    });

    it("writes a Dag field that differs from its schema default", () => {
      expect(serializeWith({ failFast: true })).toMatchObject({ fail_fast: true });
    });

    it("omits task fields left at their schema defaults", () => {
      const task = taskVar(
        serializeWith(
          {},
          { retries: 0, queue: "default", pool: "default_pool", retryDelay: 300, owner: "airflow" },
        ),
      );
      for (const key of ["retries", "queue", "pool", "retry_delay", "owner"]) {
        expect(task).not.toHaveProperty(key);
      }
    });

    it("writes task fields that differ from their schema defaults", () => {
      const task = taskVar(
        serializeWith(
          {},
          { retries: 2, queue: "typescript", retryDelay: 600, executionTimeout: 5 },
        ),
      );
      expect(task).toMatchObject({
        retries: 2,
        queue: "typescript",
        retry_delay: 600,
        execution_timeout: 5,
      });
    });

    it("never writes the email flags, which have no recipient to reach", () => {
      const task = taskVar(serializeWith({}, { emailOnFailure: false, emailOnRetry: false }));
      expect(task).not.toHaveProperty("email_on_failure");
      expect(task).not.toHaveProperty("email_on_retry");
    });
  });

  describe("downstream_task_ids", () => {
    it("inverts the recorded wiring, sorted", () => {
      const dag = new Dag("d");
      const root = place(dag, "root");
      const right = place(dag, "right", [root]);
      const left = place(dag, "left", [root]);
      place(dag, "join", [right, left]);

      const serialized = serializeDag(dag, "", ".") as Json;
      expect(taskVar(serialized, 0)["downstream_task_ids"]).toEqual(["left", "right"]);
      expect(taskVar(serialized, 1)["downstream_task_ids"]).toEqual(["join"]);
      expect(taskVar(serialized, 3)).not.toHaveProperty("downstream_task_ids");
    });

    it("counts one edge when two arguments come from the same upstream", () => {
      const dag = new Dag("d");
      const upstream = place(dag, "up");
      place(dag, "down", [upstream, upstream]);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "down",
      ]);
    });

    it("ignores a literal argument that looks like a reference", () => {
      const dag = new Dag("d");
      place(dag, "up");
      const factory = dag.task("down", async (_args: WiredArgs) => undefined);
      factory({ config: { dagId: "d", taskId: "up" } as unknown as TaskRef });

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)).not.toHaveProperty(
        "downstream_task_ids",
      );
    });
  });

  describe("order-only edges", () => {
    it("writes an edge between two tasks onto the upstream task", () => {
      const dag = new Dag("d");
      const loaded = place(dag, "load");
      const cleaned = place(dag, "cleanup");
      loaded.before(cleaned);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "cleanup",
      ]);
    });

    it("joins the wiring on one graph, since the serialized Dag has only one", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      place(dag, "transform", [extracted]);
      const cleaned = place(dag, "cleanup");
      extracted.before(cleaned);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "cleanup",
        "transform",
      ]);
    });

    it("counts one edge when the wiring already drew it", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      const transformed = place(dag, "transform", [extracted]);
      extracted.before(transformed);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "transform",
      ]);
    });
  });

  describe("task_group", () => {
    /** The Dag's root task group, as the serialized payload carries it. */
    function rootGroup(dag: Dag): Json {
      return (serializeDag(dag, "", ".") as Json)["task_group"] as Json;
    }

    it("puts a Dag's top-level tasks in the root group", () => {
      const dag = new Dag("d");
      place(dag, "alpha");
      place(dag, "beta");

      expect(rootGroup(dag)).toMatchObject({
        _group_id: null,
        prefix_group_id: true,
        children: { alpha: ["operator", "alpha"], beta: ["operator", "beta"] },
      });
    });

    it("nests a group by embedding its own object, as Python does", () => {
      const dag = new Dag("d");
      place(dag, "top");
      const staging = dag.taskGroup("staging");
      place(staging, "stage");

      const root = rootGroup(dag);
      expect(root["children"]).toMatchObject({ top: ["operator", "top"] });
      const [kind, nested] = (root["children"] as Json)["staging"] as [string, Json];
      expect(kind).toBe("taskgroup");
      expect(nested).toMatchObject({
        _group_id: "staging",
        children: { "staging.stage": ["operator", "staging.stage"] },
      });
    });

    it("keeps a grouped task out of the root group's children", () => {
      const dag = new Dag("d");
      place(dag.taskGroup("staging"), "stage");

      expect(Object.keys(rootGroup(dag)["children"] as Json)).toEqual(["staging"]);
    });

    it("nests to any depth", () => {
      const dag = new Dag("d");
      const outer = dag.taskGroup("outer");
      place(outer.taskGroup("inner"), "deep");

      const root = rootGroup(dag);
      const [, outerGroup] = (root["children"] as Json)["outer"] as [string, Json];
      const [, innerGroup] = (outerGroup["children"] as Json)["outer.inner"] as [string, Json];
      // The local segment, as Python records it: the qualified id is rebuilt
      // from where the group sits in the tree.
      expect(innerGroup).toMatchObject({
        _group_id: "inner",
        children: { "outer.inner.deep": ["operator", "outer.inner.deep"] },
      });
    });

    it("records an edge between a group and a task on the group", () => {
      const dag = new Dag("d");
      const staging = dag.taskGroup("staging");
      place(staging, "stage");
      const loaded = place(dag, "load");
      staging.before(loaded);

      const [, group] = (rootGroup(dag)["children"] as Json)["staging"] as [string, Json];
      expect(group).toMatchObject({
        downstream_task_ids: ["load"],
        upstream_task_ids: [],
        downstream_group_ids: [],
        upstream_group_ids: [],
      });
      // And expanded onto the task graph, which is the only one the scheduler
      // reads: the group's leaves carry the edge to the task it points at.
      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "load",
      ]);
    });

    it("records an edge between two groups on both of them", () => {
      const dag = new Dag("d");
      const first = dag.taskGroup("first");
      place(first, "a");
      const second = dag.taskGroup("second");
      place(second, "b");
      first.before(second);

      const children = rootGroup(dag)["children"] as Json;
      const [, firstGroup] = children["first"] as [string, Json];
      const [, secondGroup] = children["second"] as [string, Json];
      expect(firstGroup).toMatchObject({
        downstream_group_ids: ["second"],
        upstream_group_ids: [],
      });
      expect(secondGroup).toMatchObject({
        upstream_group_ids: ["first"],
        downstream_group_ids: [],
      });
    });

    it("records an upstream task on the group it points at", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      const staging = dag.taskGroup("staging");
      place(staging, "stage");
      extracted.before(staging);

      const [, group] = (rootGroup(dag)["children"] as Json)["staging"] as [string, Json];
      expect(group).toMatchObject({ upstream_task_ids: ["extract"], downstream_task_ids: [] });
    });

    it("expands an edge into a group onto its roots, not every task it holds", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      const staging = dag.taskGroup("staging");
      const staged = place(staging, "stage");
      place(staging, "check", [staged]);
      extracted.before(staging);

      // "staging.check" already runs after "staging.stage", so the edge in
      // reaches only the task that starts the group.
      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "staging.stage",
      ]);
    });

    it("expands an edge out of a group from its leaves", () => {
      const dag = new Dag("d");
      const staging = dag.taskGroup("staging");
      const staged = place(staging, "stage");
      place(staging, "check", [staged]);
      const loaded = place(dag, "load");
      staging.before(loaded);

      const serialized = serializeDag(dag, "", ".") as Json;
      // "staging.stage" keeps its own intra-group edge and nothing more: the
      // edge out of the group leaves from the task that finishes it.
      expect(taskVar(serialized, 0)["downstream_task_ids"]).toEqual(["staging.check"]);
      expect(taskVar(serialized, 1)["downstream_task_ids"]).toEqual(["load"]);
    });

    it("joins one group's leaves to the next group's roots", () => {
      const dag = new Dag("d");
      const first = dag.taskGroup("first");
      const firstHead = place(first, "head");
      place(first, "tail", [firstHead]);
      const second = dag.taskGroup("second");
      const secondHead = place(second, "head");
      place(second, "tail", [secondHead]);
      first.before(second);

      const serialized = serializeDag(dag, "", ".") as Json;
      expect(taskVar(serialized, 1)["downstream_task_ids"]).toEqual(["second.head"]);
      const children = rootGroup(dag)["children"] as Json;
      const [, secondGroup] = children["second"] as [string, Json];
      // The downstream group records the upstream's leaves as well as the
      // group itself; the upstream group records only the group edge.
      expect(secondGroup).toMatchObject({
        upstream_group_ids: ["first"],
        upstream_task_ids: ["first.tail"],
      });
      const [, firstGroup] = children["first"] as [string, Json];
      expect(firstGroup).toMatchObject({
        downstream_group_ids: ["second"],
        downstream_task_ids: [],
      });
    });

    it("reaches a task held by a nested group", () => {
      const dag = new Dag("d");
      const extracted = place(dag, "extract");
      const outer = dag.taskGroup("outer");
      place(outer.taskGroup("inner"), "deep");
      extracted.before(outer);

      expect(taskVar(serializeDag(dag, "", ".") as Json, 0)["downstream_task_ids"]).toEqual([
        "outer.inner.deep",
      ]);
    });

    it("carries an empty group, which holds nothing and constrains nothing", () => {
      const dag = new Dag("d");
      place(dag, "solo");
      dag.taskGroup("empty");

      const [, group] = (rootGroup(dag)["children"] as Json)["empty"] as [string, Json];
      expect(group).toMatchObject({ _group_id: "empty", children: {} });
    });

    it("steps over an empty group so a chain through it still orders its ends", () => {
      // An empty group has no roots and no leaves, so the two edges used to
      // expand into nothing at all and `after` ran beside `before`. Python
      // bridges the gap by walking up through the group's own upstreams.
      const dag = new Dag("d");
      const before = place(dag, "before");
      const after = place(dag, "after");
      const empty = dag.taskGroup("empty");
      before.before(empty);
      empty.before(after);

      const tasks = taskMap(serializeDag(dag, "", ".") as Json);
      expect(tasks.get("before")?.["downstream_task_ids"]).toEqual(["after"]);
    });

    it("steps over a chain of empty groups", () => {
      const dag = new Dag("d");
      const before = place(dag, "before");
      const after = place(dag, "after");
      const first = dag.taskGroup("first");
      const second = dag.taskGroup("second");
      before.before(first);
      first.before(second);
      second.before(after);

      const tasks = taskMap(serializeDag(dag, "", ".") as Json);
      expect(tasks.get("before")?.["downstream_task_ids"]).toEqual(["after"]);
    });

    it("draws nothing when an empty group has no other side to reach", () => {
      const dag = new Dag("d");
      const before = place(dag, "before");
      before.before(dag.taskGroup("empty"));

      const tasks = taskMap(serializeDag(dag, "", ".") as Json);
      expect(tasks.get("before")).not.toHaveProperty("downstream_task_ids");
    });
  });

  describe("rejects a value the schema cannot carry", () => {
    it.each([
      ["startDate", { startDate: "2026-01-01" }, /startDate for Dag "d" must be a valid Date/],
      ["an invalid Date", { startDate: new Date("nope") }, /must be a valid Date/],
      ["description", { description: 7 }, /description for Dag "d" must be a string/],
      ["catchup", { catchup: "yes" }, /catchup for Dag "d" must be a boolean/],
      [
        "maxActiveRuns",
        { maxActiveRuns: "3" },
        /maxActiveRuns for Dag "d" must be a finite number/,
      ],
      ["dagrunTimeout", { dagrunTimeout: Infinity }, /must be a duration in seconds/],
      ["tags", { tags: ["a", 2] }, /tags for Dag "d" must be an array of strings/],
    ])("on %s", (_name, spec, expected) => {
      expect(() => serializeWith(spec as DagSpec)).toThrowError(expected);
    });

    it("names the task a bad task field belongs to", () => {
      expect(() => serializeWith({}, { retries: "two" } as unknown as TaskSpec)).toThrowError(
        /retries for task "t" of Dag "d" must be a finite number/,
      );
    });
  });
});

describe("serializeValue", () => {
  it.each([
    ["a string", "x", "x"],
    ["a boolean", true, true],
    ["a number", 1.5, 1.5],
    ["null", null, null],
    ["undefined", undefined, null],
    ["a list, without a wrapper", [1, "a"], [1, "a"]],
  ])("passes %s through", (_name, value, expected) => {
    expect(serializeValue(value)).toEqual(expected);
  });

  it("encodes a Date as fractional epoch seconds", () => {
    expect(serializeValue(new Date("2026-01-01T00:00:00.500Z"))).toEqual({
      __type: "datetime",
      __var: 1767225600.5,
    });
  });

  it("encodes a Set as a sorted list", () => {
    expect(serializeValue(new Set(["gamma", "alpha", "beta"]))).toEqual({
      __type: "set",
      __var: ["alpha", "beta", "gamma"],
    });
  });

  it.each([
    ["an object", { b: 1, a: "x" }],
    [
      "a Map",
      new Map<string, unknown>([
        ["b", 1],
        ["a", "x"],
      ]),
    ],
  ])("encodes %s as a dict", (_name, value) => {
    expect(serializeValue(value)).toEqual({ __type: "dict", __var: { b: 1, a: "x" } });
  });

  it("recurses into nested values", () => {
    expect(serializeValue({ when: new Date("2026-01-01T00:00:00Z"), items: [{ n: 1 }] })).toEqual({
      __type: "dict",
      __var: {
        when: { __type: "datetime", __var: 1767225600 },
        items: [{ __type: "dict", __var: { n: 1 } }],
      },
    });
  });

  it.each([
    ["a non-finite number", Number.NaN, /non-finite number/],
    ["an invalid Date", new Date("nope"), /invalid Date/],
    ["a function", () => undefined, /Cannot serialize a function/],
  ])("rejects %s", (_name, value, expected) => {
    expect(() => serializeValue(value)).toThrowError(expected);
  });
});

describe("unwrapTypeEncoding", () => {
  it("takes the __var of an encoded value", () => {
    expect(unwrapTypeEncoding({ __type: "timedelta", __var: 300 })).toBe(300);
  });

  it.each([
    ["a primitive", 5],
    ["a list", [1, 2]],
    ["an object that is not encoded", { __var: 1 }],
  ])("leaves %s alone", (_name, value) => {
    expect(unwrapTypeEncoding(value)).toEqual(value);
  });
});

describe("computeRelativeFileloc", () => {
  it.each([
    ["a file inside the bundle", "/bundles/app/dags/bundle.mjs", "/bundles/app", "dags/bundle.mjs"],
    ["a file at the bundle root", "/bundles/app/bundle.mjs", "/bundles/app", "bundle.mjs"],
    ["a file that is the bundle", "/bundles/app", "/bundles/app", "."],
    ["an unknown bundle path", "/bundles/app/bundle.mjs", "", "."],
    ["an unknown file", "", "/bundles/app", ""],
  ])("resolves %s", (_name, fileloc, bundlePath, expected) => {
    expect(computeRelativeFileloc(fileloc, bundlePath)).toBe(expected);
  });
});
