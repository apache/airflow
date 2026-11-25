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
  getAllIterationsArgNames,
  getDagLoops,
  getDagTaskInputs,
  getDagTaskRecords,
  Label,
  type LoopRef,
  type TaskGroupRef,
  type TaskRef,
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
  report({ history: refinement.all(evaluated) });
  refinement.before(cleanup());
  return { dag, refine };
}

describe("TaskGroupRef.loop", () => {
  it("adds the gate as a task of the group once the Dag is read, named after its handler", () => {
    const { dag } = buildRefine();
    expect(getDagTaskRecords(dag).has("refine.accurateEnough")).toBe(false);

    finalizeDag(dag);
    expect(getDagLoops(dag).get("refine")).toEqual({
      groupId: "refine",
      maxIterations: 10,
      hasUntil: true,
      gateTaskId: "refine.accurateEnough",
    });
    expect(getDagTaskRecords(dag).get("refine.accurateEnough")!.operator).toMatchObject({
      taskType: "LoopGateOperator",
    });
    expect([...getDagTaskRecords(dag).keys()].at(-1)).toBe("refine.accurateEnough");
    expect(getAllIterationsArgNames(dag, "report")).toEqual(new Set(["history"]));
    expect(getAllIterationsArgNames(dag, "refine.evaluate")).toEqual(new Set());
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
    // Core finds a gate by Python's gate operator name; every other task keeps the SDK's own.
    expect(gate["task_type"]).toBe("LoopGateOperator");
    expect(gate).not.toHaveProperty("_operator_name");
    const evaluate = (serialized["tasks"] as { __var: Json }[]).find(
      ({ __var: task }) => task["task_id"] === "refine.evaluate",
    )!.__var;
    expect(evaluate["task_type"]).toBe("TypeScriptOperator");
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

  it("leaves a group holding a loop only through the loop's gate", () => {
    const dag = new Dag("ts_nested");
    const outer = dag.taskGroup("outer");
    const inner = outer.taskGroup("inner");
    inner.task("increment", async () => 1)();
    inner.loop({ maxIterations: 3 });
    dag
      .task("after", async () => undefined)()
      .after(Label.of("done").on(outer));

    expect(serialize(dag)["edge_info"]).toEqual({
      "outer.downstream_join_id": { after: { label: "done" } },
      "outer.inner.__loop_gate": { after: { label: "done" } },
    });
  });

  it("does not leave a group through a nested loop's gate whose edges out stay in the group", () => {
    const dag = new Dag("ts_gate_not_a_leaf");
    const outer = dag.taskGroup("outer");
    const inner = outer.taskGroup("inner");
    const produced = inner.task("produce", async () => 1)();
    const innerLoop = inner.loop({ maxIterations: 3 });
    outer.task(
      "sum",
      async (_: { all: XComSequence<number> }) => 1,
    )({
      all: innerLoop.all(produced),
    });
    dag
      .task("after", async () => undefined)()
      .after(Label.of("done").on(outer));

    const serialized = serialize(dag);
    expect(serialized["edge_info"]).toEqual({
      "outer.downstream_join_id": { after: { label: "done" } },
      "outer.sum": { after: { label: "done" } },
    });
    expect(downstreamOf(serialized).get("outer.inner.__loop_gate")).toEqual(["outer.sum"]);
  });

  it("leaves a plain group inside a loop body through its own tasks", () => {
    const dag = new Dag("ts_group_in_loop");
    const loop = dag.taskGroup("loop");
    const g = loop.taskGroup("g");
    g.task("a", async () => 1)();
    loop
      .task("b", async () => 1)()
      .after(g);
    loop.loop({ maxIterations: 2 });

    const downstream = downstreamOf(serialize(dag));
    expect(downstream.get("loop.g.a")).toEqual(["loop.b"]);
    expect(downstream.get("loop.b")).toEqual(["loop.__loop_gate"]);
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

    finalizeDag(dag);
    const record = getDagTaskRecords(dag).get("g.enough")!;
    expect(record.spec).toEqual({ taskId: "enough", retries: 3 });
  });

  describe("numbers the gate's id as Python's get_unique_task_id does", () => {
    it("for handler-less loops whose groups add no prefix", () => {
      const dag = new Dag("ts_unprefixed_gates");
      for (const id of ["a", "b", "c"]) {
        const group = dag.taskGroup(id, { prefixGroupId: false });
        group.task(`leaf_${id}`, async () => 1)();
        group.loop({ maxIterations: 2 });
      }

      finalizeDag(dag);
      expect([...getDagLoops(dag).values()].map((loop) => loop.gateTaskId)).toEqual([
        "__loop_gate",
        "__loop_gate__1",
        "__loop_gate__2",
      ]);
    });

    it("for a handler named like a body task, one past the highest number in use", () => {
      const dag = new Dag("ts_gate_named_like_a_task");
      const group = dag.taskGroup("g");
      const first = group.task("check", async () => 1)();
      const leaf = group.task("check__1", async () => 1)();
      first.before(leaf);
      async function check({ value }: { value: number }) {
        return value > 0;
      }
      group.loop(check, { value: leaf }, { maxIterations: 2 });

      finalizeDag(dag);
      expect(getDagLoops(dag).get("g")!.gateTaskId).toBe("g.check__2");
    });

    it("keeps an id the author gave, so a clash with it is theirs to fix", () => {
      const dag = new Dag("ts_gate_taskid_clash");
      const group = dag.taskGroup("g");
      group.task("leaf", async () => 1)();
      group.loop({ maxIterations: 2, taskId: "leaf" });

      expect(() => finalizeDag(dag)).toThrow(
        'The gate of Loop "g" cannot be added: ' +
          'Task "g.leaf" is already registered for Dag "ts_gate_taskid_clash"',
      );
    });

    it("keeps an id the author gave when a body task declared after loop() takes it", () => {
      const dag = new Dag("ts_gate_taskid_late_clash");
      const group = dag.taskGroup("g");
      group.loop({ maxIterations: 2, taskId: "leaf" });
      group.task("leaf", async () => 1)();

      expect(() => finalizeDag(dag)).toThrow(
        'The gate of Loop "g" cannot be added: ' +
          'Task "g.leaf" is already registered for Dag "ts_gate_taskid_late_clash"',
      );
    });

    it("for a handler named like a group of the loop, which Python rejects as a duplicate", () => {
      const dag = new Dag("ts_gate_named_like_a_group");
      const group = dag.taskGroup("g");
      group.taskGroup("check").task("leaf", async () => 1)();
      async function check() {
        return true;
      }

      group.loop(check, {}, { maxIterations: 2 });

      expect(() => finalizeDag(dag)).toThrow(
        'The gate of Loop "g" cannot be added: ' +
          'Task "g.check" is already registered for Dag "ts_gate_named_like_a_group"',
      );
    });

    it("once the body exists, when a body task declared after loop() takes the handler's name", () => {
      const dag = new Dag("ts_gate_before_body");
      const group = dag.taskGroup("g");
      async function check() {
        return true;
      }
      group.loop(check, {}, { maxIterations: 2 });
      const first = group.task("check", async () => 1)();
      const second = group.task("check__1", async () => 1)();
      first.before(second);

      const serialized = serialize(dag);
      expect(getDagLoops(dag).get("g")!.gateTaskId).toBe("g.check__2");
      const gate = getDagTaskRecords(dag).get("g.check__2")!;
      expect(gate.task.taskId).toBe("g.check__2");
      expect(gate.operator).toMatchObject({ taskType: "LoopGateOperator" });
      expect(groupObject(serialized, "g")["loop"]).toMatchObject({
        terminal_task_id: "g.check__1",
        gate_task_id: "g.check__2",
      });
      expect(downstreamOf(serialized).get("g.check__1")).toEqual(["g.check__2"]);
    });

    it("but not onto a group of the loop, which Python rejects as a duplicate", () => {
      const dag = new Dag("ts_gate_numbered_onto_a_group");
      const group = dag.taskGroup("g");
      async function check() {
        return true;
      }
      group.loop(check, {}, { maxIterations: 2 });
      group.taskGroup("check__1");
      group.task("check", async () => 1)();

      expect(() => finalizeDag(dag)).toThrow(
        'The gate of Loop "g" cannot be added: ' +
          'Task "g.check__1" is already registered for Dag "ts_gate_numbered_onto_a_group"',
      );
    });

    it("but not for a task outside the loop that takes it, which Python rejects as a duplicate", () => {
      const dag = new Dag("ts_gate_late_outside");
      const group = dag.taskGroup("g", { prefixGroupId: false });
      async function check() {
        return true;
      }
      group.loop(check, {}, { maxIterations: 2 });
      dag.task("check", async () => 1)();

      expect(() => finalizeDag(dag)).toThrow(
        'The gate of Loop "g" cannot be added: ' +
          'Task "check" is already registered for Dag "ts_gate_late_outside"',
      );
    });
  });

  it("checks the gate's inputs and options at the loop() call", () => {
    const group = new Dag("ts_gate_checked_early").taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    async function check() {
      return true;
    }

    expect(() => group.loop(check, leaf as never, { maxIterations: 2 })).toThrow(
      'Task "g.check" of Dag "ts_gate_checked_early" takes one object naming its inputs',
    );
    expect(() => group.loop(check, {}, { maxIterations: 2, colour: "red" } as never)).toThrow(
      'Unknown option "colour" in the spec for Dag "ts_gate_checked_early" task "g.check"',
    );
  });

  it("keeps the inputs it checked, however the author changes them afterwards", () => {
    const dag = new Dag("ts_gate_inputs_copied");
    const group = dag.taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    const inputs: Record<string, unknown> = { value: leaf };
    async function check({ value }: { value: number }) {
      return value > 0;
    }
    group.loop(check, inputs as never, { maxIterations: 2 });
    inputs["value"] = 7;
    inputs["extra"] = Label.of("late").on(leaf);

    finalizeDag(dag);
    expect(getDagTaskInputs(dag).get("g.check")).toEqual({ value: leaf });
  });

  it.each([
    ["triggerRule", "always"],
    ["dependsOnPast", true],
    ["startDate", new Date("2026-01-01")],
    ["endDate", new Date("2026-02-01")],
  ])("rejects %s on the gate, since it would change when the gate runs", (option, value) => {
    const group = new Dag("ts_gate_option").taskGroup("g");
    group.task("leaf", async () => 1)();

    expect(() => group.loop({ maxIterations: 2, [option]: value } as never)).toThrow(
      `The options of Loop "g" of Dag "ts_gate_option" cannot set ${option}`,
    );
  });

  it("falls back to Python's gate name for a handler with no usable name", () => {
    const dag = new Dag("ts_anonymous_gate");
    const group = dag.taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    group.loop(({ value }: { value: number }) => value > 0, { value: leaf }, { maxIterations: 2 });

    finalizeDag(dag);
    expect(getDagLoops(dag).get("g")!.gateTaskId).toBe("g.__loop_gate");
  });

  it.each([
    ["a non-ASCII name", "準備"],
    ["a name that is no identifier", "check-done"],
  ])("falls back to Python's gate name for %s, as Python does", (_, name) => {
    const dag = new Dag("ts_odd_gate");
    const group = dag.taskGroup("g");
    const leaf = group.task("leaf", async () => 1)();
    const until = async () => true;
    Object.defineProperty(until, "name", { value: name });
    group.loop(until, { value: leaf } as never, { maxIterations: 2 });

    finalizeDag(dag);
    expect(getDagLoops(dag).get("g")!.gateTaskId).toBe("g.__loop_gate");
  });

  it("dispatches the gate to the runtime as an operator", () => {
    const { dag } = buildRefine();

    expect(getBundleTask(new Bundle(dag), "ts_refine", "refine.accurateEnough")).toMatchObject({
      kind: "operator",
      operator: { operatorName: "LoopGateOperator", taskType: "LoopGateOperator" },
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

  describe("a body task's reference", () => {
    it("is read every iteration through its loop's all(), the same reference on every call", () => {
      const dag = new Dag("ts_loop_all");
      const group = dag.taskGroup("g");
      const direct = group.task("direct", async () => 1)();
      const nested = group.taskGroup("sub").task("nested", async () => 1)();
      direct.before(nested);
      const loop = group.loop({ maxIterations: 2 });

      expect(loop.all(direct)).toBe(loop.all(direct));
      expect(loop.all(nested).taskId).toBe("g.sub.nested");
    });

    it.each([
      [
        "a task of a plain group",
        (dag: Dag) => dag.taskGroup("plain").task("leaf", async () => 1)(),
      ],
      ["a task of the Dag", (dag: Dag) => dag.task("top", async () => 1)()],
      [
        "a task of another loop",
        (dag: Dag) => {
          const other = dag.taskGroup("other");
          const task = other.task("leaf", async () => 1)();
          other.loop({ maxIterations: 2 });
          return task;
        },
      ],
      [
        "a task of another Dag",
        () => new Dag("ts_elsewhere").taskGroup("g").task("leaf", async () => 1)(),
      ],
    ])("is not read through all() of a loop it is not in: %s", (_label, make) => {
      const dag = new Dag("ts_loop_all_rejects");
      const group = dag.taskGroup("g");
      group.task("leaf", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });

      expect(() => loop.all(make(dag))).toThrow(
        /all\(\) of loop "g" of Dag "ts_loop_all_rejects" takes a task of the loop, and ".*" is not one/,
      );
    });

    it("is read through all() only as a task reference", () => {
      const group = new Dag("ts_loop_all_value").taskGroup("g");
      const leaf = group.task("leaf", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });

      expect(() => loop.all({ dagId: "d", taskId: "g.leaf" } as never)).toThrow(
        /takes a task reference, not/,
      );
      expect(() => loop.all(loop.all(leaf))).toThrow(/takes a task reference, not/);
    });

    it("is read every iteration through all(), as a plain xcom binding with the edge from the gate", () => {
      const serialized = serialize(buildRefine().dag);

      const report = (serialized["tasks"] as { __var: Json }[]).find(
        ({ __var: task }) => task["task_id"] === "report",
      )!.__var;
      expect(report["_arg_bindings"]).toEqual([
        { name: "history", kind: "xcom", task_id: "refine.evaluate" },
      ]);
      expect(downstreamOf(serialized).get("refine.accurateEnough")).toContain("report");
    });

    it.each([
      [
        "bare outside its loop",
        (body: TaskRef<number>, dag: Dag) => {
          dag.task("outside", async (_: { value: number }) => undefined)({ value: body });
        },
        /Input "value" of task "outside" .* takes "g.leaf", a task of loop "g", from outside that loop.*all\(\) on the reference loop\(\) returned for "g"/,
      ],
      [
        "with all() inside its own loop",
        (body: TaskRef<number>, _dag: Dag, group: TaskGroupRef, loop: LoopRef) => {
          group.task(
            "inside",
            async (_: { value: XComSequence<number> }) => 1,
          )({
            value: loop.all(body),
          });
        },
        /Input "value" of task "g.inside" .* with all\(\) from inside loop "g"/,
      ],
    ])("is rejected when read %s", (_label, read, message) => {
      const dag = new Dag("ts_loop_reads");
      const group = dag.taskGroup("g");
      const body = group.task("leaf", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });
      read(body, dag, group, loop);

      expect(() => finalizeDag(dag)).toThrow(message);
    });
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

    it("a body whose tasks wait for each other, which leaves no task to end it", () => {
      const dag = new Dag("ts_body_cycle");
      const group = dag.taskGroup("g");
      const a = group.task("a", async () => 1)();
      const b = group.task("b", async () => 1)();
      a.before(b);
      b.before(a);
      group.loop({ maxIterations: 2 });
      expect(() => finalizeDag(dag)).toThrow(
        'but its tasks wait for each other: "g.a" -> "g.b" -> "g.a"',
      );
    });

    it("an order-only edge from a body task to a task outside the loop", () => {
      const dag = new Dag("ts_body_edge");
      const group = dag.taskGroup("g");
      const leaf = group.task("leaf", async () => 1)();
      const outside = dag.task("outside", async () => undefined)();
      group.loop({ maxIterations: 2 });
      leaf.before(outside);
      expect(() => finalizeDag(dag)).toThrow(
        /"g.leaf" of Dag "ts_body_edge" is in loop "g".*through the reference loop\(\) returned for "g"/,
      );
    });

    it("a body task that depends on a task reading the loop through all()", () => {
      const dag = new Dag("ts_loop_cycle");
      const group = dag.taskGroup("g");
      const first = group.task("first", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });
      const reader = dag.task(
        "reader",
        async (_: { all: XComSequence<number> }) => 1,
      )({
        all: loop.all(first),
      });
      group.task(
        "last",
        async (_: { fromReader: number; first: number }) => 1,
      )({
        fromReader: reader,
        first,
      });

      expect(() => finalizeDag(dag)).toThrow(
        'Loop "g" of Dag "ts_loop_cycle" would wait for itself: ' +
          '"g.__loop_gate" -> "reader" -> "g.last" -> "g.__loop_gate"',
      );
    });

    it("a body task that runs after a task the loop is ordered before, with no all() read", () => {
      const dag = new Dag("ts_order_cycle");
      const group = dag.taskGroup("g");
      const a = group.task("a", async () => 1)();
      const b = group.task("b", async () => 1)();
      a.before(b);
      const loop = group.loop({ maxIterations: 2 });
      const x = dag.task("x", async () => undefined)();
      loop.before(x);
      x.before(a);

      expect(() => finalizeDag(dag)).toThrow(
        'Loop "g" of Dag "ts_order_cycle" would wait for itself: ' +
          '"g.__loop_gate" -> "x" -> "g.a" -> "g.b" -> "g.__loop_gate". ' +
          "A task downstream of the loop runs after its last iteration",
      );
    });

    it.each([
      ["before() on it", (every: TaskRef, outside: TaskRef) => every.before(outside)],
      ["after() on it", (every: TaskRef, outside: TaskRef) => every.after(outside)],
      [
        "it as the argument of before()",
        (every: TaskRef, outside: TaskRef) => outside.before(every),
      ],
      ["it as the argument of after()", (every: TaskRef, outside: TaskRef) => outside.after(every)],
    ])("an edge drawn with %s, where the loop's own reference orders the loop", (_label, draw) => {
      const dag = new Dag("ts_all_edge");
      const group = dag.taskGroup("g");
      const leaf = group.task("leaf", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });
      const outside = dag.task("outside", async () => undefined)();

      expect(() => draw(loop.all(leaf), outside)).toThrow(
        /The all\(\) of "g.leaf" of Dag "ts_all_edge" cannot be used with before\(\) or after\(\).*reference loop\(\) returned/,
      );
      expect(downstreamOf(serialize(dag)).get("g.leaf")).toEqual(["g.__loop_gate"]);
    });

    it.each([
      ["Label.of(...).on()", (_dag: Dag, every: TaskRef) => Label.of("x").on(every)],
      [
        "then() or else()",
        (dag: Dag, every: TaskRef) =>
          dag.if(async () => true, undefined, { taskId: "decide" }).then(every),
      ],
      [
        "case()",
        (dag: Dag, every: TaskRef) =>
          dag.switch(async () => every, undefined, { taskId: "decide" }).case(every),
      ],
    ])("the reference all() returned, given to %s", (use, place) => {
      const dag = new Dag("ts_all_placed");
      const group = dag.taskGroup("g");
      const leaf = group.task("leaf", async () => 1)();
      const loop = group.loop({ maxIterations: 2 });

      expect(() => place(dag, loop.all(leaf))).toThrow(
        `The all() of "g.leaf" of Dag "ts_all_placed" cannot be used with ${use}. Order the ` +
          "whole loop through the reference loop() returned",
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
