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
import { Dag, getDagTaskRecords, type TaskRef } from "../../src/sdk/dag.js";
import { DagRegistry } from "../../src/sdk/registry.js";

describe("Dag", () => {
  it("returns a frozen TaskRef handle with the Dag and task identity", () => {
    const dag = new Dag("example_dag");
    const task = dag.task("my_task", async () => "hello");
    expect(task).toEqual({ dagId: "example_dag", taskId: "my_task" });
    expect(Object.isFrozen(task)).toBe(true);
  });

  it("chains upstream handles into downstream task inputs", () => {
    const dag = new Dag("chained_dag");
    const extracted = dag.task("extract", async () => ({ rows: 1 }));
    const transformed = dag.task("transform", async () => undefined, { inputs: { extracted } });
    const loaded = dag.task("load", async () => undefined, { inputs: { transformed }, spec: {} });

    expect(extracted).toEqual({ dagId: "chained_dag", taskId: "extract" });
    expect(transformed).toEqual({ dagId: "chained_dag", taskId: "transform" });
    expect(loaded).toEqual({ dagId: "chained_dag", taskId: "load" });

    const records = getDagTaskRecords(dag);
    expect(records.get("extract")?.inputs).toEqual({});
    expect(records.get("transform")?.inputs).toEqual({ extracted });
    expect(records.get("load")?.inputs).toEqual({ transformed });
  });

  it("accepts several named inputs for one task", () => {
    const dag = new Dag("fan_in_dag");
    const extracted = dag.task("extract", async () => undefined);
    const otherTaskResult = dag.task("other_task", async () => undefined);
    dag.task("transform", async () => undefined, { inputs: { extracted, otherTaskResult } });

    expect(getDagTaskRecords(dag).get("transform")?.inputs).toEqual({
      extracted,
      otherTaskResult,
    });
  });

  it("records frozen inputs that later mutation of the caller's object cannot change", () => {
    const dag = new Dag("example_dag");
    const extracted = dag.task("extract", async () => undefined);
    const inputs: Record<string, TaskRef> = { extracted };
    dag.task("transform", async () => undefined, { inputs });

    inputs.sneaky = extracted;
    const recorded = getDagTaskRecords(dag).get("transform")!.inputs;
    expect(recorded).toEqual({ extracted });
    expect(Object.isFrozen(recorded)).toBe(true);
  });

  it("rejects an input taken from another Dag", () => {
    const first = new Dag("first_dag");
    const second = new Dag("second_dag");
    const extracted = first.task("extract", async () => undefined);
    expect(() =>
      second.task("transform", async () => undefined, { inputs: { extracted } }),
    ).toThrowError(
      /Input "extracted" of task "transform" comes from Dag "first_dag", not "second_dag"/,
    );
  });

  it.each([
    ["a plain string", "extract"],
    ["an object without a dagId", { taskId: "extract" }],
    ["null", null],
  ])("rejects an input that is not a task handle: %s", (_label, value) => {
    const dag = new Dag("example_dag");
    const extracted = value as unknown as TaskRef;
    expect(() =>
      dag.task("transform", async () => undefined, { inputs: { extracted } }),
    ).toThrowError(
      /Input "extracted" of task "transform" must be a task handle returned by dag\.task\(\.\.\.\)/,
    );
    expect(getDagTaskRecords(dag).has("transform")).toBe(false);
  });

  it("rejects an input referring to a task that is not registered yet", () => {
    const dag = new Dag("example_dag");
    expect(() =>
      dag.task("transform", async () => undefined, {
        inputs: { ghost: { dagId: "example_dag", taskId: "ghost" } },
      }),
    ).toThrowError(/Input "ghost" of task "transform" refers to unregistered task "ghost"/);
  });

  it("retains its spec and each task's handler and spec, copied and frozen", () => {
    const dagSpec = {};
    const taskSpec = {};
    const handler = async () => "hello";
    const dag = new Dag("example_dag", dagSpec);
    dag.task("my_task", handler, { spec: taskSpec });

    expect(dag.dagId).toBe("example_dag");
    expect(dag.spec).toEqual(dagSpec);
    expect(Object.isFrozen(dag.spec)).toBe(true);
    const record = getDagTaskRecords(dag).get("my_task");
    expect(record?.handler).toBe(handler);
    expect(record?.spec).toEqual(taskSpec);
    expect(Object.isFrozen(record!.spec)).toBe(true);
  });

  it.each([
    ["a populated object", { schedule: "@daily" }],
    ["null", null],
    ["an array", []],
  ])("rejects a Dag spec that is not an empty object: %s", (_label, spec) => {
    expect(() => new Dag("example_dag", spec as unknown as Record<string, never>)).toThrowError(
      /spec for Dag "example_dag" must be an empty object/,
    );
  });

  it.each([
    ["a populated object", { retries: 2 }],
    ["null", null],
    ["an array", []],
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
    ["a misspelled inputs key", { input: {} }],
    ["a misspelled spec key", { specs: {} }],
    ["an upstream handle passed positionally", { upstream: { dagId: "d", taskId: "t" } }],
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
    ["a string", "inputs"],
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
    firstDag.task("extract", first);
    secondDag.task("extract", second);

    const registry = new DagRegistry();
    registry.register(firstDag, secondDag);
    expect(registry.getTaskHandler("first_dag", "extract")).toBe(first);
    expect(registry.getTaskHandler("second_dag", "extract")).toBe(second);
  });

  it("rejects an empty dagId", () => {
    expect(() => new Dag("")).toThrowError(/dagId must be made of alphanumeric/);
  });

  it("rejects an empty taskId", () => {
    const dag = new Dag("example_dag");
    expect(() => dag.task("", async () => undefined)).toThrowError(
      /taskId must be made of alphanumeric/,
    );
  });

  it.each(["   ", "\t", "my dag", "a/b", "task@1"])(
    "rejects a dagId with characters no Python dag_id allows: %j",
    (dagId) => {
      expect(() => new Dag(dagId)).toThrowError(/dagId must be made of alphanumeric/);
    },
  );

  it.each(["   ", "\t", "my task", "a/b", "task@1"])(
    "rejects a taskId with characters no Python task_id allows: %j",
    (taskId) => {
      const dag = new Dag("example_dag");
      expect(() => dag.task(taskId, async () => undefined)).toThrowError(
        /taskId must be made of alphanumeric/,
      );
    },
  );

  it("rejects a dagId longer than 250 characters", () => {
    expect(() => new Dag("d".repeat(251))).toThrowError(
      /dagId must be less than 250 characters, not 251/,
    );
  });

  it("rejects a taskId longer than 250 characters", () => {
    const dag = new Dag("example_dag");
    expect(() => dag.task("t".repeat(251), async () => undefined)).toThrowError(
      /taskId must be less than 250 characters, not 251/,
    );
  });

  it("accepts a Unicode dagId that Python's word-character rule allows", () => {
    const handler = async () => undefined;
    const dag = new Dag("café_dag");
    dag.task("任務", handler);
    const registry = new DagRegistry();
    registry.register(dag);
    expect(registry.getTaskHandler("café_dag", "任務")).toBe(handler);
  });

  it("rejects non-function handlers", () => {
    const dag = new Dag("example_dag");
    expect(() => dag.task("x", "not a function" as unknown as () => Promise<unknown>)).toThrowError(
      /must be a function/,
    );
  });

  it("treats a dotted TaskGroup taskId as a single taskId (group.task)", () => {
    const dag = new Dag("example_dag");
    dag.task("transforms.normalize", async () => "ok");
    const registry = new DagRegistry();
    registry.register(dag);
    expect(registry.getTaskHandler("example_dag", "transforms.normalize")).toBeDefined();
    // Should NOT accidentally match the prefix alone
    expect(registry.getTaskHandler("example_dag", "transforms")).toBeUndefined();
    expect(registry.getTaskHandler("example_dag", "normalize")).toBeUndefined();
  });
});
