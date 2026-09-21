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
import { serializeDag } from "../../src/coordinator/serde.js";
import { Bundle, finalizeBundleDags } from "../../src/sdk/bundle.js";
import { Dag, getDagOrderEdges, getDagTaskRecords } from "../../src/sdk/dag.js";
import type { TriggerDagRunSpec } from "../../src/sdk/operators.js";

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
  it("returns the task's reference, since there is nothing to call", () => {
    const { dag, trigger } = triggered();

    expect(trigger).toMatchObject({ dagId: "d", taskId: "trigger_downstream" });
    expect(dag.taskIds).toEqual(["trigger_downstream"]);
  });

  it("carries no TypeScript handler, because a Python worker runs it", () => {
    const { dag } = triggered();

    const record = getDagTaskRecords(dag).get("trigger_downstream")!;
    expect(record.fn).toBeUndefined();
    expect(record.operator?.taskType).toBe("TriggerDagRunOperator");
    // Nothing for the runtime to dispatch, so it is never routed here.
    expect(new Bundle(dag).getTaskHandler("d", "trigger_downstream")).toBeUndefined();
  });

  it("counts as called, so the Dag is complete without a factory call", () => {
    const { dag } = triggered();

    expect(() => finalizeBundleDags(new Bundle(dag))).not.toThrow();
  });

  it("serializes as the Python operator, with its arguments renamed", () => {
    const { dag } = triggered({ waitForCompletion: true, pokeInterval: 30 });

    expect(serialize(dag).tasks.get("trigger_downstream")).toMatchObject({
      task_id: "trigger_downstream",
      task_type: "TriggerDagRunOperator",
      _task_module: "airflow.providers.standard.operators.trigger_dagrun",
      trigger_dag_id: "downstream_etl",
      wait_for_completion: true,
      poke_interval: 30,
    });
  });

  it("carries what the UI needs to draw the task, which Python reads off the class", () => {
    const { dag } = triggered();

    expect(serialize(dag).tasks.get("trigger_downstream")).toMatchObject({
      template_fields: [
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "conf",
        "wait_for_completion",
        "skip_when_already_exists",
      ],
      ui_color: "#ffefeb",
      template_fields_renderers: { conf: "py" },
      _operator_extra_links: { "Triggered DAG": "_link_TriggerDagRunLink" },
    });
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

  it("passes a templated argument through as plain JSON, not an encoded dict", () => {
    const { dag } = triggered({ conf: { day: "{{ ds }}", nested: { n: [1, 2] } } });

    expect(serialize(dag).tasks.get("trigger_downstream")?.["conf"]).toEqual({
      day: "{{ ds }}",
      nested: { n: [1, 2] },
    });
  });

  it("leaves an unset option to the Python operator's own default", () => {
    const { dag } = triggered({ runId: undefined });

    expect(serialize(dag).tasks.get("trigger_downstream")).not.toHaveProperty("trigger_run_id");
  });

  it("is not marked as a TypeScript task, so nothing routes it to this runtime", () => {
    const { dag } = triggered();

    const task = serialize(dag).tasks.get("trigger_downstream")!;
    expect(task).not.toHaveProperty("language");
    expect(task).not.toHaveProperty("is_stub");
    expect(task).not.toHaveProperty("_arg_bindings");
  });

  it("inherits no queue from the Dag, so a Python worker can pick it up", () => {
    const dag = new Dag("d", { queue: "typescript" });
    dag.task("extract", async () => undefined)();
    dag.triggerDagRun({ taskId: "trigger_downstream", dagId: "downstream_etl" });

    const { tasks } = serialize(dag);
    expect(tasks.get("extract")?.["queue"]).toBe("typescript");
    expect(tasks.get("trigger_downstream")).not.toHaveProperty("queue");
  });

  it("still takes a task spec, including a queue the author chooses", () => {
    const { dag } = triggered({}, { retries: 2, queue: "python_heavy" });

    expect(serialize(dag).tasks.get("trigger_downstream")).toMatchObject({
      retries: 2,
      queue: "python_heavy",
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

    it("an option the Python constructor does not declare", () => {
      // It would reach the worker as a TypeError when the Dag runs, long after
      // it parsed.
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
        /conf of task "trigger_downstream" of Dag "d"\.bad is .*, which a Python operator argument cannot carry/,
      );
    });

    it("a condition built on a trigger task, which has no handler to decide with", () => {
      const { dag, trigger } = triggered();
      const other = dag.task("other", async () => undefined)();

      expect(() => dag.if(trigger as never).then(other)).toThrowError(
        /Task "trigger_downstream" of Dag "d" is a Python operator, so it cannot decide a branch/,
      );
    });
  });
});
