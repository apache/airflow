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

import { afterEach, describe, expect, it, vi } from "vitest";
import { serializeDag } from "../../src/coordinator/serde.js";
import { Bundle, finalizeBundleDags, getBundleTrigger } from "../../src/sdk/bundle.js";
import { Dag, getDagOrderEdges, getDagTaskRecords } from "../../src/sdk/dag.js";
import type { TriggerDagRunSpec } from "../../src/sdk/trigger-dag-run.js";

type Json = Record<string, unknown>;

/** The serialized Dag, and its tasks keyed by task id. */
function serialize(dag: Dag) {
  const serialized = serializeDag(dag, "", ".") as Json;
  const tasks = serialized["tasks"] as { __var: Json }[];
  return {
    serialized,
    tasks: new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var])),
  };
}

/** A Dag holding one trigger task, built from `spec`. */
function triggered(spec: Partial<TriggerDagRunSpec> = {}, taskSpec = {}) {
  const dag = new Dag("d");
  const trigger = dag.triggerDagRun(
    { taskId: "trigger_downstream", dagId: "downstream_etl", ...spec } as TriggerDagRunSpec,
    taskSpec,
  );
  return { dag, trigger };
}

describe("dag.triggerDagRun", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns the task's reference, since there is nothing to call", () => {
    const { dag, trigger } = triggered();

    expect(trigger).toMatchObject({ dagId: "d", taskId: "trigger_downstream" });
    expect(dag.taskIds).toEqual(["trigger_downstream"]);
  });

  it("carries no handler, and its options with the operator's defaults", () => {
    const { dag } = triggered();

    const record = getDagTaskRecords(dag).get("trigger_downstream")!;
    expect(record.fn).toBeUndefined();
    expect(record.trigger).toEqual({
      dagId: "downstream_etl",
      runId: undefined,
      conf: undefined,
      resetDagRun: false,
      waitForCompletion: false,
      pokeInterval: 60,
      allowedStates: ["success"],
      failedStates: ["failed"],
      skipWhenAlreadyExists: false,
      failWhenDagIsPaused: false,
      note: undefined,
      deferrable: false,
    });
    const bundle = new Bundle(dag);
    expect(bundle.getTaskHandler("d", "trigger_downstream")).toBeUndefined();
    expect(getBundleTrigger(bundle, "d", "trigger_downstream")).toBe(record.trigger);
  });

  it("falls back on the default for empty allowedStates, not for empty failedStates", () => {
    const { dag } = triggered({ allowedStates: [], failedStates: [] });

    expect(getDagTaskRecords(dag).get("trigger_downstream")!.trigger).toMatchObject({
      allowedStates: ["success"],
      failedStates: [],
    });
  });

  it.each([
    ["True", {}, true],
    ["false", {}, false],
    ["1", { deferrable: false }, false],
    ["f", { deferrable: true }, true],
  ])(
    "with AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE=%s and %o, defers: %s",
    (value, spec, deferrable) => {
      vi.stubEnv("AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE", value);

      const { dag } = triggered(spec);

      expect(getDagTaskRecords(dag).get("trigger_downstream")!.trigger).toMatchObject({
        deferrable,
      });
    },
  );

  it("counts as called, so the Dag is complete without a factory call", () => {
    const { dag } = triggered();

    expect(() => finalizeBundleDags(new Bundle(dag))).not.toThrow();
  });

  it("serializes as a TypeScript task, drawn as TriggerDagRunOperator", () => {
    const { dag } = triggered({ waitForCompletion: true, pokeInterval: 30 });

    const task = serialize(dag).tasks.get("trigger_downstream")!;
    expect(task).toMatchObject({
      task_id: "trigger_downstream",
      task_type: "TypeScriptOperator",
      _task_module: "airflow.sdk.coordinators.node",
      language: "typescript",
      is_stub: true,
      template_fields: [],
      _operator_name: "TriggerDagRunOperator",
      ui_color: "#ffefeb",
      _operator_extra_links: { "Triggered DAG": "_link_TriggerDagRunLink" },
    });
    // The runtime reads its options from the Dag it built, not from the Dag JSON.
    expect(task).not.toHaveProperty("trigger_dag_id");
    expect(task).not.toHaveProperty("wait_for_completion");
    expect(task).not.toHaveProperty("_arg_bindings");
  });

  it("records the triggered Dag as a dependency of this one", () => {
    const { dag } = triggered();

    expect(serialize(dag).serialized["dag_dependencies"]).toEqual([
      {
        source: "d",
        target: "downstream_etl",
        label: "trigger_downstream",
        dependency_type: "trigger",
        dependency_id: "trigger_downstream",
      },
    ]);
  });

  it("inherits the Dag's queue, so it runs where the Dag's other tasks run", () => {
    const dag = new Dag("d", { queue: "typescript" });
    dag.task("extract", async () => undefined)();
    dag.triggerDagRun({ taskId: "trigger_downstream", dagId: "downstream_etl" });

    const { tasks } = serialize(dag);
    expect(tasks.get("extract")?.["queue"]).toBe("typescript");
    expect(tasks.get("trigger_downstream")?.["queue"]).toBe("typescript");
  });

  it("still takes a task spec", () => {
    const { dag } = triggered({}, { retries: 2, queue: "typescript_heavy" });

    expect(serialize(dag).tasks.get("trigger_downstream")).toMatchObject({
      retries: 2,
      queue: "typescript_heavy",
    });
  });

  it("participates in before and after like any task", () => {
    const dag = new Dag("d");
    const loaded = dag.task("load", async () => undefined)();
    const trigger = dag.triggerDagRun({ taskId: "trigger_downstream", dagId: "downstream_etl" });

    trigger.after(loaded);

    expect(getDagOrderEdges(dag)).toEqual([{ upstream: "load", downstream: "trigger_downstream" }]);
    expect(serialize(dag).tasks.get("load")?.["downstream_task_ids"]).toEqual([
      "trigger_downstream",
    ]);
  });

  it("sits in a task group like any task, prefix included", () => {
    const dag = new Dag("d");
    dag.taskGroup("publish").triggerDagRun({ taskId: "trigger", dagId: "downstream_etl" });

    expect(dag.taskIds).toEqual(["publish.trigger"]);
  });

  describe("rejects", () => {
    it.each([
      ["no taskId", {}],
      ["an empty taskId", { taskId: "" }],
      ["a taskId that is not a string", { taskId: 7 }],
    ])("%s", (_label, spec) => {
      const dag = new Dag("d");

      expect(() =>
        dag.triggerDagRun({ dagId: "downstream_etl", ...spec } as TriggerDagRunSpec),
      ).toThrowError(/A triggerDagRun task of Dag "d" has no taskId/);
    });

    it.each([
      ["no dagId", {}],
      ["an empty dagId", { dagId: "" }],
      ["a dagId that is not a string", { dagId: 7 }],
    ])("%s", (_label, spec) => {
      const dag = new Dag("d");

      expect(() =>
        dag.triggerDagRun({ taskId: "t", ...spec } as unknown as TriggerDagRunSpec),
      ).toThrowError(/triggerDagRun\(\.\.\.\) needs the dagId of the Dag to trigger/);
    });

    it.each([
      ["null", null],
      ["a string", "downstream_etl"],
      ["an array", []],
    ])("%s where the options belong", (_label, spec) => {
      const dag = new Dag("d");

      expect(() => dag.triggerDagRun(spec as unknown as TriggerDagRunSpec)).toThrowError(
        /A triggerDagRun task of Dag "d" has no taskId/,
      );
    });

    it("an option the operator does not declare", () => {
      const dag = new Dag("d");

      expect(() =>
        dag.triggerDagRun({
          taskId: "t",
          dagId: "d2",
          waitFor: true,
        } as unknown as TriggerDagRunSpec),
      ).toThrowError(/Unknown option "waitFor" for triggerDagRun\(\.\.\.\)/);
    });

    it.each([
      ["a Date", new Date("2026-01-01")],
      ["a Map", new Map()],
      ["a function", () => 1],
    ])("%s inside conf, which the Dag JSON cannot carry", (_label, value) => {
      const { dag } = triggered({ conf: { bad: value } as never });

      expect(() => serialize(dag)).toThrowError(
        /conf of task "trigger_downstream" of Dag "d"\.bad is .*, which a Dag run's conf cannot carry/,
      );
    });

    it.each([
      ["allowedStates", { allowedStates: ["done"] }],
      ["failedStates", { failedStates: ["error"] }],
    ])("a %s entry that is not a Dag run state", (name, spec) => {
      const dag = new Dag("d");

      expect(() =>
        dag.triggerDagRun({ taskId: "t", dagId: "d2", ...spec } as unknown as TriggerDagRunSpec),
      ).toThrowError(new RegExp(`option "${name}" holds "\\w+", which is not a Dag run state`));
    });

    it.each([
      ["a negative pokeInterval", { pokeInterval: -1 }, /"pokeInterval" must be a non-negative/],
      ["a non-boolean deferrable", { deferrable: "yes" }, /"deferrable" must be a boolean/],
      ["a non-string runId", { runId: 7 }, /"runId" must be a string/],
      ["a conf that is an array", { conf: [1] }, /"conf" must be an object/],
    ])("%s", (_label, spec, error) => {
      const dag = new Dag("d");

      expect(() =>
        dag.triggerDagRun({ taskId: "t", dagId: "d2", ...spec } as unknown as TriggerDagRunSpec),
      ).toThrowError(error);
    });

    it("an AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE that is not a boolean", () => {
      vi.stubEnv("AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE", "yes");

      expect(() => triggered()).toThrowError(
        'AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE is "yes", which is not a boolean; use true or false',
      );
    });

    it("a condition built on a trigger task, which has no handler to decide with", () => {
      const { dag, trigger } = triggered();
      const other = dag.task("other", async () => undefined)();

      expect(() => dag.if(trigger as never).then(other)).toThrowError(
        /Task "trigger_downstream" of Dag "d" is a triggerDagRun task, so it cannot decide a branch/,
      );
    });
  });
});
