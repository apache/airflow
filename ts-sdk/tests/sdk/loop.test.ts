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
import { getBundleTask, Bundle } from "../../src/sdk/bundle.js";
import {
  Dag,
  finalizeDag,
  findTaskLoop,
  getDagLoops,
  getDagTaskRecords,
  type TaskGroupRef,
} from "../../src/sdk/dag.js";
import type { XComSequence } from "../../src/sdk/xcom-sequence.js";

type Json = Record<string, unknown>;
type Metrics = { estimate: number; error: number };

function serialize(dag: Dag): Json {
  finalizeDag(dag);
  return serializeDag(dag, "", ".") as Json;
}

function downstreamOf(serialized: Json): Map<string, string[]> {
  const tasks = serialized["tasks"] as { __var: Json }[];
  return new Map(
    tasks.map(({ __var: task }) => [
      task["task_id"] as string,
      (task["downstream_task_ids"] as string[] | undefined) ?? [],
    ]),
  );
}

function groupObject(serialized: Json, groupId: string): Json {
  const root = serialized["task_group"] as Json;
  const [, group] = (root["children"] as Record<string, [string, Json]>)[groupId]!;
  return group;
}

/** ADR-0003's example: improve → evaluate in the body, a gate on the error, a report outside. */
function buildRefine(): { dag: Dag; refine: TaskGroupRef } {
  const dag = new Dag("ts_refine", { queue: "typescript" });
  const refine = dag.taskGroup("refine");
  const improve = refine.task(async function improve(): Promise<number> {
    return 1.5;
  });
  const evaluate = refine.task(async function evaluate({
    estimate,
  }: {
    estimate: number;
  }): Promise<Metrics> {
    return { estimate, error: Math.abs(estimate * estimate - 2) };
  });
  async function accurateEnough({ result }: { result: Metrics }) {
    return { stop: result.error < 1e-6 };
  }
  const report = dag.task(async function report(_: { history: XComSequence<Metrics> }) {});
  const cleanup = dag.task(async function cleanup() {});

  const evaluated = evaluate({ estimate: improve() });
  const refinement = refine.loop(accurateEnough, { result: evaluated }, { maxIterations: 10 });
  report({ history: evaluated });
  refinement.before(cleanup());
  return { dag, refine };
}

describe("TaskGroupRef.loop", () => {
  it("declares the gate as a task of the group, named after its handler", () => {
    const { dag } = buildRefine();

    expect(getDagLoops(dag).get("refine")).toEqual({
      groupId: "refine",
      maxIterations: 10,
      hasUntil: true,
      gateTaskId: "refine.accurateEnough",
    });
    expect(getDagTaskRecords(dag).get("refine.accurateEnough")!.loopGate).toEqual({
      hasUntil: true,
    });
    expect(findTaskLoop(dag, "refine.improve")?.groupId).toBe("refine");
    expect(findTaskLoop(dag, "report")).toBeUndefined();
  });

  it("serializes the loop group with the four fields core reads", () => {
    const serialized = serialize(buildRefine().dag);

    expect(groupObject(serialized, "refine")["loop"]).toEqual({
      max_iterations: 10,
      has_until: true,
      terminal_task_id: "refine.evaluate",
      gate_task_id: "refine.accurateEnough",
    });
    // The gate reads the terminal task as wired data, as any task reads an upstream.
    const gate = (serialized["tasks"] as { __var: Json }[]).find(
      ({ __var: task }) => task["task_id"] === "refine.accurateEnough",
    )!.__var;
    expect(gate["_arg_bindings"]).toEqual([
      { name: "result", kind: "xcom", task_id: "refine.evaluate" },
    ]);
    expect(gate["queue"]).toBe("typescript");
  });

  it("makes the gate the loop's only way out, keeping the data reference to the body task", () => {
    const serialized = serialize(buildRefine().dag);
    const downstream = downstreamOf(serialized);

    expect(downstream.get("refine.improve")).toEqual(["refine.evaluate"]);
    // The edge report took from evaluate now leaves from the gate, as Python's
    // redirect_outside_edges_to_gate does, and the loop's own edge reaches cleanup from the gate.
    expect(downstream.get("refine.evaluate")).toEqual(["refine.accurateEnough"]);
    expect(downstream.get("refine.accurateEnough")).toEqual(["cleanup", "report"]);
    const report = (serialized["tasks"] as { __var: Json }[]).find(
      ({ __var: task }) => task["task_id"] === "report",
    )!.__var;
    expect(report["_arg_bindings"]).toEqual([
      { name: "history", kind: "xcom", task_id: "refine.evaluate" },
    ]);
  });

  it("runs a fixed count with a gate of Python's default name after the terminal task", () => {
    const dag = new Dag("ts_poll");
    const poll = dag.taskGroup("poll");
    const check = poll.task("check", async () => true);
    const before = dag.task("before", async () => undefined)();
    const after = dag.task("after", async () => undefined)();
    check();
    poll.loop({ maxIterations: 3 }).after(before).before(after);

    const serialized = serialize(dag);
    expect(groupObject(serialized, "poll")["loop"]).toEqual({
      max_iterations: 3,
      has_until: false,
      terminal_task_id: "poll.check",
      gate_task_id: "poll.__loop_gate",
    });
    const downstream = downstreamOf(serialized);
    expect(downstream.get("poll.check")).toEqual(["poll.__loop_gate"]);
    // An edge into the loop reaches its body, never the gate, and one out leaves from the gate.
    expect(downstream.get("before")).toEqual(["poll.check"]);
    expect(downstream.get("poll.__loop_gate")).toEqual(["after"]);
  });

  it("takes the gate's id and options from the spec", () => {
    const dag = new Dag("ts_named_gate");
    const group = dag.taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    group.loop(
      async ({ value }: { value: number }) => value > 0,
      { value: leaf },
      {
        maxIterations: 2,
        taskId: "enough",
        retries: 3,
      },
    );

    const record = getDagTaskRecords(dag).get("g.enough")!;
    expect(record.spec).toEqual({ taskId: "enough", retries: 3 });
  });

  it("falls back to Python's gate name for a handler with no usable name", () => {
    const dag = new Dag("ts_anonymous_gate");
    const group = dag.taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    group.loop(({ value }: { value: number }) => value > 0, { value: leaf }, { maxIterations: 2 });

    expect(getDagLoops(dag).get("g")!.gateTaskId).toBe("g.__loop_gate");
  });

  it("dispatches the gate to the runtime as a loop gate", () => {
    const { dag } = buildRefine();

    expect(getBundleTask(new Bundle(dag), "ts_refine", "refine.accurateEnough")).toMatchObject({
      kind: "loopGate",
      hasUntil: true,
    });
    expect(getBundleTask(new Bundle(dag), "ts_refine", "refine.improve")).toMatchObject({
      kind: "handler",
      dag,
    });
  });

  it("is a node that labels and conditions resolve to the group", () => {
    const dag = new Dag("ts_loop_ref");
    const group = dag.taskGroup("g");
    group.task("leaf", async () => 1)();
    const loop = group.loop({ maxIterations: 2 });
    const decide = async () => true;
    const then = dag.task("then", async () => undefined)();

    expect(() => dag.if(decide).then(loop as never)).toThrow(/has to be a task, not a task group/);
    expect(loop.before(then)).toBe(loop);
  });

  describe("rejects", () => {
    it.each([
      [0, /must be a positive integer, not the number 0/],
      [1.5, /must be a positive integer, not the number 1.5/],
      ["3", /must be a positive integer, not the string "3"/],
      [undefined, /must be a positive integer, not undefined/],
    ])("maxIterations %s", (maxIterations, message) => {
      const group = new Dag("ts_bad_bound").taskGroup("g");
      expect(() => group.loop({ maxIterations } as never)).toThrow(message);
    });

    it("a second loop() on one group", () => {
      const group = new Dag("ts_twice").taskGroup("g");
      group.loop({ maxIterations: 2 });
      expect(() => group.loop({ maxIterations: 2 })).toThrow(/was already declared/);
    });

    it("a loop() after the Dag was read", () => {
      const dag = new Dag("ts_late");
      const group = dag.taskGroup("g");
      group.task("leaf", async () => 1)();
      finalizeDag(dag);
      expect(() => group.loop({ maxIterations: 2 })).toThrow(/after the Dag was read/);
    });

    it.each([
      [[async () => true, {}], /takes loop\(until, inputs, \{ maxIterations \}\), not 2 arguments/],
      [[{ maxIterations: 2 }, {}], /takes either loop\(\{ maxIterations \}\)/],
      [[async () => true, {}, null], /options of Loop "g" of Dag "ts_args" must be an object/],
    ])("a call of the wrong shape %#", (args, message) => {
      const group = new Dag("ts_args").taskGroup("g");
      expect(() => (group.loop as (...a: unknown[]) => unknown)(...args)).toThrow(message);
    });

    it("a loop nested in another", () => {
      const dag = new Dag("ts_nested");
      const outer = dag.taskGroup("outer");
      const inner = outer.taskGroup("inner");
      inner.task("leaf", async () => 1)();
      inner.loop({ maxIterations: 2 });
      outer.loop({ maxIterations: 2 });
      expect(() => finalizeDag(dag)).toThrow(
        /Loop "outer.inner" of Dag "ts_nested" is inside loop "outer"/,
      );
    });

    it("a body that ends in two tasks", () => {
      const dag = new Dag("ts_two_leaves");
      const group = dag.taskGroup("g");
      group.task("a", async () => 1)();
      group.task("b", async () => 1)();
      group.loop({ maxIterations: 2 });
      expect(() => finalizeDag(dag)).toThrow(
        /has to end in exactly one task.*but "g.a", "g.b" all end it/,
      );
    });

    it("a body that holds no task", () => {
      const dag = new Dag("ts_empty");
      dag.taskGroup("g").loop({ maxIterations: 2 });
      expect(() => finalizeDag(dag)).toThrow(/but it holds no task/);
    });

    it("an order-only edge from a body task to a task outside the loop", () => {
      const dag = new Dag("ts_body_edge");
      const group = dag.taskGroup("g");
      const leaf = group.task("leaf", async () => 1)();
      const outside = dag.task("outside", async () => undefined)();
      group.loop({ maxIterations: 2 });
      leaf.before(outside);
      expect(() => finalizeDag(dag)).toThrow(
        /"g.leaf" of Dag "ts_body_edge" is in loop "g".*with the reference loop\(\) returned: g.before/,
      );
    });

    it("an order-only edge from a group nested in the body to a task outside", () => {
      const dag = new Dag("ts_subgroup_edge");
      const group = dag.taskGroup("g");
      const sub = group.taskGroup("sub");
      sub.task("leaf", async () => 1)();
      const outside = dag.task("outside", async () => undefined)();
      group.loop({ maxIterations: 2 });
      sub.before(outside);
      expect(() => finalizeDag(dag)).toThrow(/"g.sub" of Dag "ts_subgroup_edge" is in loop "g"/);
    });
  });

  it("allows an edge into the body from outside, and edges within it", () => {
    const dag = new Dag("ts_edges_in");
    const group = dag.taskGroup("g");
    const outside = dag.task("outside", async () => 1)();
    const first = group.task("first", async (_: { seed: number }) => 1)({ seed: outside });
    const second = group.task("second", async () => 2)();
    first.before(second);
    group.loop({ maxIterations: 2 });

    const downstream = downstreamOf(serialize(dag));
    expect(downstream.get("outside")).toEqual(["g.first"]);
    expect(downstream.get("g.first")).toEqual(["g.second"]);
    expect(downstream.get("g.second")).toEqual(["g.__loop_gate"]);
  });
});
