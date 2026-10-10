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
import { Bundle, finalizeBundleDags, getBundleTask } from "../../src/sdk/bundle.js";
import {
  Dag,
  finalizeDag,
  getDagOrderEdges,
  getDagTaskRecords,
  type TaskRecord,
} from "../../src/sdk/dag.js";
import { triggerDagRun, type TriggerDagRunSpec } from "../../src/sdk/trigger-dag-run.js";

type Json = Record<string, unknown>;

function serialize(dag: Dag) {
  const serialized = serializeDag(dag, "", ".") as Json;
  const tasks = serialized["tasks"] as { __var: Json }[];
  return {
    serialized,
    tasks: new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var])),
  };
}

function triggered(spec: Partial<TriggerDagRunSpec> = {}, taskSpec = {}) {
  const dag = new Dag("d");
  const trigger = dag.task(
    triggerDagRun({ dagId: "downstream_etl", ...spec } as TriggerDagRunSpec),
    { taskId: "trigger_downstream", ...taskSpec },
  )();
  return { dag, trigger };
}

describe("triggerDagRun", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("is placed by calling the factory dag.task returns, like any task", () => {
    const { dag, trigger } = triggered();

    expect(trigger).toMatchObject({ dagId: "d", taskId: "trigger_downstream" });
    expect(dag.taskIds).toEqual(["trigger_downstream"]);
  });

  it("carries no handler, and its options with the operator's defaults", () => {
    const { dag } = triggered();

    const record = getDagTaskRecords(dag).get("trigger_downstream")!;
    expect(record.fn).toBeUndefined();
    expect(record.operator).toMatchObject({
      operatorName: "TriggerDagRunOperator",
      dagId: "downstream_etl",
      runId: undefined,
      logicalDate: undefined,
      runAfter: undefined,
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
    expect(getBundleTask(bundle, "d", "trigger_downstream")).toEqual({
      kind: "operator",
      operator: record.operator,
      dag,
    });
  });

  it("falls back on the default for empty allowedStates, not for empty failedStates", () => {
    const { dag } = triggered({ allowedStates: [], failedStates: [] });

    expect(getDagTaskRecords(dag).get("trigger_downstream")!.operator).toMatchObject({
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

      expect(getDagTaskRecords(dag).get("trigger_downstream")!.operator).toMatchObject({
        deferrable,
      });
    },
  );

  it("is complete once the factory is called", () => {
    const { dag } = triggered();

    expect(() => finalizeBundleDags(new Bundle(dag))).not.toThrow();
  });

  it("keeps a task record to either a handler or an operator", () => {
    const { dag } = triggered();
    const record = getDagTaskRecords(dag).get("trigger_downstream")!;

    // @ts-expect-error -- a record cannot carry both a handler and an operator.
    const both: TaskRecord = { ...record, fn: async () => undefined };

    expect(both.operator).toBe(record.operator);
  });

  it("takes its id positionally too", () => {
    const dag = new Dag("d");
    dag.task("trigger_downstream", triggerDagRun({ dagId: "downstream_etl" }))();

    expect(dag.taskIds).toEqual(["trigger_downstream"]);
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
    dag.task(triggerDagRun({ dagId: "downstream_etl" }), { taskId: "trigger_downstream" })();

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
    const trigger = dag.task(triggerDagRun({ dagId: "downstream_etl" }), {
      taskId: "trigger_downstream",
    })();

    trigger.after(loaded);

    expect(getDagOrderEdges(dag)).toEqual([{ upstream: "load", downstream: "trigger_downstream" }]);
    expect(serialize(dag).tasks.get("load")?.["downstream_task_ids"]).toEqual([
      "trigger_downstream",
    ]);
  });

  it("sits in a task group like any task, prefix included", () => {
    const dag = new Dag("d");
    dag
      .taskGroup("publish")
      .task(triggerDagRun({ dagId: "downstream_etl" }), { taskId: "trigger" })();

    expect(dag.taskIds).toEqual(["publish.trigger"]);
  });

  describe("rejects", () => {
    it("no taskId", () => {
      const dag = new Dag("d");

      expect(() => dag.task(triggerDagRun({ dagId: "downstream_etl" }), {})).toThrowError(
        /A triggerDagRun task of Dag "d" has no taskId/,
      );
    });

    it("inputs given to a trigger task", () => {
      const dag = new Dag("d");
      const factory = dag.task(triggerDagRun({ dagId: "downstream_etl" }), {
        taskId: "trigger_downstream",
      }) as unknown as (inputs: unknown) => unknown;

      expect(() => factory({ rows: 1 })).toThrowError(
        /Task "trigger_downstream" of Dag "d" is a triggerDagRun task, which takes no inputs/,
      );
    });

    it("a trigger task left uncalled, when the Dag is read", () => {
      const dag = new Dag("d");
      dag.task(triggerDagRun({ dagId: "downstream_etl" }), { taskId: "trigger_downstream" });

      expect(() => finalizeDag(dag)).toThrowError(/trigger_downstream/);
    });

    it.each([
      ["no dagId", {}],
      ["an empty dagId", { dagId: "" }],
      ["a dagId that is not a string", { dagId: 7 }],
    ])("%s", (_label, spec) => {
      expect(() => triggerDagRun(spec as unknown as TriggerDagRunSpec)).toThrowError(
        /triggerDagRun\(\.\.\.\) needs the dagId of the Dag to trigger/,
      );
    });

    it.each([
      ["null", null],
      ["a string", "downstream_etl"],
      ["an array", []],
    ])("%s where the options belong", (_label, spec) => {
      expect(() => triggerDagRun(spec as unknown as TriggerDagRunSpec)).toThrowError(
        /triggerDagRun\(\.\.\.\) takes an options object/,
      );
    });

    it("an option the operator does not declare", () => {
      expect(() =>
        triggerDagRun({ dagId: "d2", waitFor: true } as unknown as TriggerDagRunSpec),
      ).toThrowError(/Unknown option "waitFor" for triggerDagRun\(\.\.\.\)/);
    });

    it.each([
      ["a Date", new Date("2026-01-01")],
      ["a Map", new Map()],
      ["a function", () => 1],
    ])("%s inside conf, which the Dag JSON cannot carry", (_label, value) => {
      const { dag } = triggered({ conf: { bad: value } as never });

      expect(() => serialize(dag)).toThrowError(
        /conf of task "trigger_downstream" of Dag "d" holds .*, which JSON cannot carry/,
      );
    });

    it.each([
      ["allowedStates", { allowedStates: ["done"] }],
      ["failedStates", { failedStates: ["error"] }],
    ])("a %s entry that is not a Dag run state", (name, spec) => {
      expect(() =>
        triggerDagRun({ dagId: "d2", ...spec } as unknown as TriggerDagRunSpec),
      ).toThrowError(new RegExp(`option "${name}" holds "\\w+", which is not a Dag run state`));
    });

    it.each([
      ["a negative pokeInterval", { pokeInterval: -1 }, /"pokeInterval" must be a non-negative/],
      ["a non-boolean deferrable", { deferrable: "yes" }, /"deferrable" must be a boolean/],
      ["a non-string runId", { runId: 7 }, /"runId" must be a string/],
      ["a conf that is an array", { conf: [1] }, /"conf" must be an object/],
      [
        "a non-Date logicalDate",
        { logicalDate: "2026-01-01" },
        /"logicalDate" must be a valid Date/,
      ],
      [
        "an invalid Date runAfter",
        { runAfter: new Date("invalid") },
        /"runAfter" must be a valid Date/,
      ],
    ])("%s", (_label, spec, error) => {
      expect(() =>
        triggerDagRun({ dagId: "d2", ...spec } as unknown as TriggerDagRunSpec),
      ).toThrowError(error);
    });

    it("an AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE that is not a boolean", () => {
      vi.stubEnv("AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE", "yes");

      expect(() => triggered()).toThrowError(
        'AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE is "yes", which is not a boolean; use true or false',
      );
    });
  });
});
