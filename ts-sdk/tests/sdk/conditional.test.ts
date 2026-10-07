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

import { describe, expect, it, vi } from "vitest";
import {
  Dag,
  finalizeDag,
  getDagOrderEdges,
  getDagTaskInputs,
  getDagTaskRecords,
  type TaskRef,
} from "../../src/sdk/dag.js";
import { serializeDag } from "../../src/coordinator/serde.js";
import type { TaskClient } from "../../src/sdk/client.js";
import { runInTaskScope, type TaskContext } from "../../src/sdk/task.js";

function gatedDag(dagId = "d", holds: boolean | unknown = true) {
  const dag = new Dag(dagId);
  const ifReady = dag.task("load_if_ready", async () => undefined)();
  const fallback = dag.task("load_fallback", async () => undefined)();
  const gated = dag.if(async () => holds as boolean, undefined, { taskId: "has_rows" });
  return { dag, ifReady, fallback, gated };
}

async function runHandler(dag: Dag, taskId: string, args: unknown = {}) {
  const skipDownstreamTasks = vi.fn(async () => undefined);
  const setXCom = vi.fn(async () => undefined);
  const client = { skipDownstreamTasks, setXCom } as unknown as TaskClient;
  const ctx = { dagId: dag.dagId, taskId } as unknown as TaskContext;
  const handler = getDagTaskRecords(dag).get(taskId)!.fn;
  const returned = await runInTaskScope({ ctx, client }, () =>
    (handler as (args: unknown) => Promise<unknown>)(args),
  );
  return { returned, skipDownstreamTasks, setXCom };
}

describe("dag.if", () => {
  it("serializes the condition as a task that decides skips", () => {
    const { dag, ifReady, fallback, gated } = gatedDag();
    gated.then(ifReady).else(fallback);

    const tasks = (serializeDag(dag, "", ".") as { tasks: { __var: Record<string, unknown> }[] })
      .tasks;
    const byId = new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
    expect(byId.get("has_rows")?.["_can_skip_downstream"]).toBe(true);
    expect(byId.get("load_if_ready")).not.toHaveProperty("_can_skip_downstream");
  });

  it("serializes the spec given to the condition", () => {
    const { dag, ifReady } = gatedDag();
    dag
      .if(async () => true, undefined, { taskId: "is_weekday", retries: 2, queue: "q" })
      .then(ifReady);

    const tasks = (serializeDag(dag, "", ".") as { tasks: { __var: Record<string, unknown> }[] })
      .tasks;
    expect(tasks.find(({ __var }) => __var["task_id"] === "is_weekday")?.__var).toMatchObject({
      retries: 2,
      queue: "q",
    });
  });

  it("draws an order-only edge to each branch, since no value flows", () => {
    const { dag, ifReady, fallback, gated } = gatedDag();
    gated.then(ifReady).else(fallback);

    expect(getDagOrderEdges(dag)).toEqual([
      { upstream: "has_rows", downstream: "load_if_ready" },
      { upstream: "has_rows", downstream: "load_fallback" },
    ]);
  });

  it("keeps the condition's own arguments and its return value", async () => {
    const dag = new Dag("d");
    const loaded = dag.task("load", async () => undefined)();
    const extracted = dag.task("extract", async () => 3)();
    dag
      .if(
        async ({ rows }: { rows: number }) => rows > 0,
        { rows: extracted },
        { taskId: "has_rows" },
      )
      .then(loaded);

    expect(getDagTaskInputs(dag).get("has_rows")).toEqual({ rows: extracted });
    expect((await runHandler(dag, "has_rows", { rows: 2 })).returned).toBe(true);
    expect((await runHandler(dag, "has_rows", { rows: 0 })).returned).toBe(false);
  });

  it("takes its inputs without a task id", () => {
    const dag = new Dag("d");
    const loaded = dag.task("load", async () => undefined)();
    const extracted = dag.task("extract", async () => 3)();
    async function hasRows({ rows }: { rows: number }): Promise<boolean> {
      return rows > 0;
    }
    dag.if(hasRows, { rows: extracted }).then(loaded);

    expect(getDagTaskInputs(dag).get("hasRows")).toEqual({ rows: extracted });
  });

  it("requires the handler's inputs at compile time", () => {
    const dag = new Dag("d");
    async function hasRows({ rows }: { rows: number }): Promise<boolean> {
      return rows > 0;
    }

    // @ts-expect-error -- hasRows needs { rows }.
    expect(() => dag.if(hasRows, undefined, { taskId: "has_rows" })).not.toThrow();
  });

  it("takes the condition's id from the handler's name", () => {
    const dag = new Dag("d");
    const loaded = dag.task("load", async () => undefined)();
    async function hasRows(): Promise<boolean> {
      return true;
    }
    const gate = dag.if(hasRows);
    gate.then(loaded);

    expect(gate.taskId).toBe("hasRows");
    expect(dag.taskIds).toEqual(["load", "hasRows"]);
  });

  describe("at run time", () => {
    it("skips the else branch when the condition holds", async () => {
      const { dag, ifReady, fallback, gated } = gatedDag();
      gated.then(ifReady).else(fallback);

      const { skipDownstreamTasks, setXCom } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_fallback"]);
      expect(setXCom).toHaveBeenCalledWith({
        key: "skipmixin_key",
        value: { skipped: ["load_fallback"] },
      });
    });

    it("skips the then branch when it does not", async () => {
      const { dag, ifReady, fallback, gated } = gatedDag("d", false);
      gated.then(ifReady).else(fallback);

      const { skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_if_ready"]);
    });

    it("skips nothing, and records nothing, when a one-sided condition holds", async () => {
      const { dag, ifReady, gated } = gatedDag();
      gated.then(ifReady);

      const { skipDownstreamTasks, setXCom } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).not.toHaveBeenCalled();
      expect(setXCom).not.toHaveBeenCalled();
    });

    it("skips only its own branch when a one-sided condition fails", async () => {
      const { dag, ifReady, gated } = gatedDag("d", false);
      gated.then(ifReady);

      const { skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_if_ready"]);
    });

    it.each([
      ["a string", "yes"],
      ["null", null],
      ["a number", 1],
      ["a bigint", 1n],
    ])("fails the task when the condition returns %s", async (_label, value) => {
      const { dag, ifReady, gated } = gatedDag("d", value);
      gated.then(ifReady);

      await expect(runHandler(dag, "has_rows")).rejects.toThrow(
        /Condition "has_rows" of Dag "d" returned .* rather than a boolean/,
      );
    });
  });

  it("carries edges of its own, like any task", () => {
    const { dag, ifReady, gated } = gatedDag();
    const extracted = dag.task("extract", async () => undefined)();
    gated.then(ifReady);

    gated.after(extracted);

    expect(getDagOrderEdges(dag)).toContainEqual({
      upstream: "extract",
      downstream: "has_rows",
    });
  });

  it("stands at the other end of an edge, as Go's .After(gate) does", () => {
    const { dag, ifReady, gated } = gatedDag();
    const notified = dag.task("notify", async () => undefined)();
    gated.then(ifReady);

    notified.after(gated);

    expect(getDagOrderEdges(dag)).toContainEqual({ upstream: "has_rows", downstream: "notify" });
  });

  it("takes another condition as a branch", () => {
    const { dag, ifReady, gated } = gatedDag();
    const inner = dag.if(async () => true, undefined, { taskId: "is_weekday" });
    inner.then(ifReady);

    gated.then(inner);

    expect(getDagOrderEdges(dag)).toContainEqual({
      upstream: "has_rows",
      downstream: "is_weekday",
    });
  });

  describe("rejects", () => {
    it("a task group as a branch", () => {
      const { dag, gated } = gatedDag();
      const group = dag.taskGroup("staging");

      expect(() => gated.then(group as never)).toThrowError(
        /The "then" branch of Dag "d" condition "has_rows" has to be a task, not a task group/,
      );
    });

    it("a branch taken from another Dag", () => {
      const { gated } = gatedDag("here");
      const { ifReady: foreign } = gatedDag("there");

      expect(() => gated.then(foreign)).toThrowError(
        /the "then" branch of "has_rows" cannot reach Dag "there" node "load_if_ready"/,
      );
    });

    it.each([
      ["a plain object", { dagId: "d", taskId: "load_if_ready" }],
      ["a string", "load_if_ready"],
      ["null", null],
    ])("%s where a branch belongs", (_label, value) => {
      const { gated } = gatedDag();

      expect(() => gated.then(value as TaskRef)).toThrowError(
        /the "then" branch of "has_rows" on Dag "d" takes tasks and task groups/,
      );
    });

    it("both branches naming the same task, which decides nothing", () => {
      const { ifReady, gated } = gatedDag();

      expect(() => gated.then(ifReady).else(ifReady)).toThrowError(
        /Both branches of Dag "d" condition "has_rows" are "load_if_ready", so the condition decides nothing/,
      );
    });

    it("a second else branch", () => {
      const { ifReady, fallback, gated } = gatedDag();
      const chain = gated.then(ifReady);
      chain.else(fallback);

      expect(() => chain.else(fallback)).toThrowError(
        /Condition "has_rows" of Dag "d" already has an? "else" branch/,
      );
    });

    it("a second then branch", () => {
      const { ifReady, fallback, gated } = gatedDag();
      gated.then(ifReady);

      expect(() => gated.then(fallback)).toThrowError(
        /Condition "has_rows" of Dag "d" already has a "then" branch/,
      );
    });

    it("a condition that names no branch, when the Dag is read", () => {
      const { dag } = gatedDag();

      expect(() => finalizeDag(dag)).toThrowError(
        /Condition "has_rows" of Dag "d" names no branch, so it decides nothing/,
      );
    });

    it("being awaited, which would resolve the chain rather than build it", async () => {
      const { gated } = gatedDag();

      await expect(Promise.resolve(gated as unknown as Promise<void>)).rejects.toThrow(
        /dag.if\(\.\.\.\) of Dag "d" was awaited/,
      );
    });
  });
});
