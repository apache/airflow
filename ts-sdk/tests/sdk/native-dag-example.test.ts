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

// The shipped native Dag example, built and serialized.
//
// The unit suites cover each construct on a Dag written for that construct
// alone; this reads the example an author is pointed at, so the graph the docs
// describe is the graph the SDK produces. The Airflow and Kubernetes suites
// run the same Dag end to end.

import { describe, expect, it } from "vitest";
import { serializeDag } from "../../src/coordinator/serde.js";
import { Bundle, listBundleNativeDags } from "../../src/sdk/bundle.js";
import { finalizeDag, getDagOrderEdges, type Dag } from "../../src/sdk/dag.js";
import { dag } from "../../example/src/native.js";

type Json = Record<string, unknown>;

const FILELOC = "/bundles/example/bundle.min.mjs";

/** The serialized Dag, and its tasks keyed by task id. */
function serialized(target: Dag) {
  const json = serializeDag(target, FILELOC, "bundle.min.mjs") as Json;
  const tasks = json["tasks"] as { __var: Json }[];
  return { json, tasks: new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var])) };
}

describe("the native Dag example", () => {
  it("declares its schedule, options and queue in TypeScript", () => {
    expect(dag.dagId).toBe("typescript_native_example");
    expect(dag.spec).toMatchObject({
      schedule: "@daily",
      catchup: false,
      tags: ["typescript", "native"],
      queue: "typescript",
    });
  });

  it("builds a graph, not a chain", () => {
    expect([...dag.taskIds].sort()).toEqual([
      "cleanup",
      "extract.north",
      "extract.south",
      "has_rows",
      "load_rows",
      "pick_cadence",
      "publish_daily",
      "publish_weekly",
      "report_empty",
      "summarize",
      "trigger_downstream",
    ]);
  });

  it("is fully laid out, so reading it raises nothing", () => {
    expect(() => finalizeDag(dag)).not.toThrow();
  });

  it("is served by the same bundle as the mixed-language handlers", () => {
    // What `main.ts` builds: importing it would start the runtime, so the
    // registration is rebuilt here rather than imported.
    const bundle = new Bundle(dag);

    expect(listBundleNativeDags(bundle).map((served) => served.dagId)).toEqual([
      "typescript_native_example",
    ]);
  });

  describe("serializes", () => {
    it("a named fan-in as an edge from each upstream into one task", () => {
      const { tasks } = serialized(dag);

      expect(tasks.get("extract.north")?.["downstream_task_ids"]).toContain("summarize");
      expect(tasks.get("extract.south")?.["downstream_task_ids"]).toContain("summarize");
      expect(tasks.get("summarize")?.["downstream_task_ids"]).toEqual(["has_rows"]);
    });

    it("the fan-in's arguments as the bindings the API server resolves", () => {
      expect(serialized(dag).tasks.get("summarize")?.["_arg_bindings"]).toEqual([
        { name: "north", kind: "xcom", task_id: "extract.north" },
        { name: "south", kind: "xcom", task_id: "extract.south" },
      ]);
    });

    it("the positional call's argument as one binding on the upstream", () => {
      // The label is whatever named the parameter, which only the packer can
      // read; the order and the upstream are what bind.
      expect(serialized(dag).tasks.get("has_rows")?.["_arg_bindings"]).toMatchObject([
        { kind: "xcom", task_id: "summarize" },
      ]);
    });

    it("the task group, with its tasks prefixed and nested under it", () => {
      const root = serialized(dag).json["task_group"] as Json;
      const [kind, group] = (root["children"] as Json)["extract"] as [string, Json];

      expect(kind).toBe("taskgroup");
      expect(group).toMatchObject({
        _group_id: "extract",
        children: {
          "extract.north": ["operator", "extract.north"],
          "extract.south": ["operator", "extract.south"],
        },
      });
    });

    it("the group edge, expanded onto the tasks the group leaves from", () => {
      // `extract.before(picked)` is drawn against the group, and reaches the
      // task graph through the group's leaves — both extract tasks, since
      // neither runs after the other inside it.
      expect(getDagOrderEdges(dag)).toContainEqual({
        upstream: "extract",
        downstream: "pick_cadence",
      });

      const { tasks } = serialized(dag);
      expect(tasks.get("extract.north")?.["downstream_task_ids"]).toContain("pick_cadence");
      expect(tasks.get("extract.south")?.["downstream_task_ids"]).toContain("pick_cadence");
    });

    it("the conditional's control edges as ordinary edges, and its skip marker", () => {
      // Nothing names the branch in the Dag JSON: the decision is a run-time
      // skip, and `_can_skip_downstream` is what makes a cleared branch stay
      // skipped.
      const { tasks } = serialized(dag);
      const hasRows = tasks.get("has_rows")!;

      expect(hasRows["downstream_task_ids"]).toEqual(["load_rows", "report_empty"]);
      expect(hasRows["_can_skip_downstream"]).toBe(true);
      expect(tasks.get("load_rows")?.["downstream_task_ids"]).toEqual(["cleanup"]);
    });

    it("the multi-way branch's cases as ordinary edges, and its skip marker", () => {
      const pick = serialized(dag).tasks.get("pick_cadence")!;

      expect(pick["downstream_task_ids"]).toEqual(["publish_daily", "publish_weekly"]);
      expect(pick["_can_skip_downstream"]).toBe(true);
    });

    it("cleanup behind every branch outcome, which is what its all_done rule is for", () => {
      const { tasks } = serialized(dag);

      for (const outcome of ["load_rows", "report_empty", "publish_daily", "publish_weekly"]) {
        expect(tasks.get(outcome)?.["downstream_task_ids"]).toContain("cleanup");
      }
      expect(tasks.get("cleanup")?.["trigger_rule"]).toBe("all_done");
      expect(tasks.get("cleanup")?.["downstream_task_ids"]).toEqual(["trigger_downstream"]);
    });

    it("the trigger task as the Python operator a Python worker runs", () => {
      const trigger = serialized(dag).tasks.get("trigger_downstream")!;

      expect(trigger).toMatchObject({
        task_type: "TriggerDagRunOperator",
        _task_module: "airflow.providers.standard.operators.trigger_dagrun",
        trigger_dag_id: "typescript_example",
      });
      // No TypeScript marker, and no queue of this Dag's, so a Python worker
      // can pick it up.
      expect(trigger).not.toHaveProperty("language");
      expect(trigger).not.toHaveProperty("queue");
    });

    it("the triggered Dag as a dependency of this one", () => {
      expect(serialized(dag).json["dag_dependencies"]).toEqual([
        {
          source: "typescript_native_example",
          target: "typescript_example",
          label: "trigger_downstream",
          dependency_type: "trigger",
          dependency_id: "trigger_downstream",
        },
      ]);
    });

    it("every other task as one this runtime executes, on the Dag's queue", () => {
      const { tasks } = serialized(dag);
      const typescript = [...tasks.values()].filter((task) => task["language"] === "typescript");

      expect(typescript).toHaveLength(dag.taskIds.length - 1);
      expect(typescript.every((task) => task["queue"] === "typescript")).toBe(true);
    });

    it("the schedule as the expanded cron the scheduler rebuilds", () => {
      expect(serialized(dag).json["timetable"]).toMatchObject({
        __type: "airflow.timetables.trigger.CronTriggerTimetable",
        __var: { expression: "0 0 * * *" },
      });
    });
  });
});
