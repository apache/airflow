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
  getDagTaskRecords,
  type TaskRef,
} from "../../src/sdk/dag.js";
import { serializeDag } from "../../src/coordinator/serde.js";
import type { TaskClient } from "../../src/sdk/client.js";
import { runInTaskScope, type TaskContext } from "../../src/sdk/task.js";

/** A Dag with two placed tasks and a placed boolean condition between them. */
function gatedDag(dagId = "d", holds: boolean | unknown = true) {
  const dag = new Dag(dagId);
  const ifReady = dag.task("load_if_ready", async () => undefined)();
  const fallback = dag.task("load_fallback", async () => undefined)();
  const gated = dag.task("has_rows", async () => holds as boolean)();
  return { dag, ifReady, fallback, gated };
}

/** Run a registered handler with a client in scope, as the runtime does. */
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
  it("leaves the condition an ordinary task of the Dag", () => {
    const { dag, ifReady, fallback, gated } = gatedDag();
    dag.if(gated).then(ifReady).else(fallback);

    expect(dag.taskIds).toEqual(["load_if_ready", "load_fallback", "has_rows"]);
  });

  it("marks the condition as deciding skips, which Airflow reads on a clear", () => {
    const { dag, ifReady, gated } = gatedDag();
    dag.if(gated).then(ifReady);

    expect(getDagTaskRecords(dag).get("has_rows")?.canSkipDownstream).toBe(true);
    expect(getDagTaskRecords(dag).get("load_if_ready")?.canSkipDownstream).toBeUndefined();
  });

  it("serializes the condition as a task that decides skips", () => {
    const { dag, ifReady, fallback, gated } = gatedDag();
    dag.if(gated).then(ifReady).else(fallback);

    const tasks = (serializeDag(dag, "", ".") as { tasks: { __var: Record<string, unknown> }[] })
      .tasks;
    const byId = new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
    expect(byId.get("has_rows")?.["_can_skip_downstream"]).toBe(true);
    expect(byId.get("load_if_ready")).not.toHaveProperty("_can_skip_downstream");
  });

  it("draws an order-only edge to each branch, since no value flows", () => {
    const { dag, ifReady, fallback, gated } = gatedDag();
    dag.if(gated).then(ifReady).else(fallback);

    expect(getDagOrderEdges(dag)).toEqual([
      { upstream: "has_rows", downstream: "load_if_ready" },
      { upstream: "has_rows", downstream: "load_fallback" },
    ]);
  });

  it("draws one edge for a one-sided condition", () => {
    const { dag, ifReady, gated } = gatedDag();
    dag.if(gated).then(ifReady);

    expect(getDagOrderEdges(dag)).toEqual([{ upstream: "has_rows", downstream: "load_if_ready" }]);
  });

  it("keeps the condition's own arguments and its return value", async () => {
    const dag = new Dag("d");
    const loaded = dag.task("load", async () => undefined)();
    const extract = dag.task("extract", async () => 3);
    const condition = dag.task("has_rows", async ({ rows }: { rows: number }) => rows > 0);
    const gated = condition({ rows: extract() });
    dag.if(gated).then(loaded);

    expect((await runHandler(dag, "has_rows", { rows: 2 })).returned).toBe(true);
    expect((await runHandler(dag, "has_rows", { rows: 0 })).returned).toBe(false);
  });

  describe("at run time", () => {
    it("skips the else branch when the condition holds", async () => {
      const { dag, ifReady, fallback, gated } = gatedDag();
      dag.if(gated).then(ifReady).else(fallback);

      const { skipDownstreamTasks, setXCom } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_fallback"]);
      // Recorded so a cleared "load_fallback" is skipped again rather than run.
      expect(setXCom).toHaveBeenCalledWith({
        key: "skipmixin_key",
        value: { skipped: ["load_fallback"] },
      });
    });

    it("skips the then branch when it does not", async () => {
      const { dag, ifReady, fallback, gated } = gatedDag("d", false);
      dag.if(gated).then(ifReady).else(fallback);

      const { skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_if_ready"]);
    });

    it("skips nothing, and records nothing, when a one-sided condition holds", async () => {
      const { dag, ifReady, gated } = gatedDag();
      dag.if(gated).then(ifReady);

      const { skipDownstreamTasks, setXCom } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).not.toHaveBeenCalled();
      expect(setXCom).not.toHaveBeenCalled();
    });

    it("skips only its own branch when a one-sided condition fails", async () => {
      // Not the whole downstream closure: this is a branch with one candidate,
      // so a task several branches converge on keeps running.
      const { dag, ifReady, gated } = gatedDag("d", false);
      dag.if(gated).then(ifReady);

      const { skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_if_ready"]);
    });

    it("reads the branches at run time, so the chain can be written after", async () => {
      const { dag, ifReady, fallback, gated } = gatedDag();
      const chain = dag.if(gated);
      chain.then(ifReady).else(fallback);

      const { skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_fallback"]);
    });

    it("awaits a condition that resolves later", async () => {
      const dag = new Dag("d");
      const ifReady = dag.task("load_if_ready", async () => undefined)();
      const fallback = dag.task("load_fallback", async () => undefined)();
      const gated = dag.task("has_rows", async () => {
        await Promise.resolve();
        return false;
      })();
      dag.if(gated).then(ifReady).else(fallback);

      const { returned, skipDownstreamTasks } = await runHandler(dag, "has_rows");

      expect(returned).toBe(false);
      expect(skipDownstreamTasks).toHaveBeenCalledWith(["load_if_ready"]);
    });

    it.each([
      ["a string", "yes"],
      ["null", null],
      ["a number", 1],
    ])("fails the task when the condition returns %s", async (_label, value) => {
      const { dag, ifReady, gated } = gatedDag("d", value);
      dag.if(gated).then(ifReady);

      await expect(runHandler(dag, "has_rows")).rejects.toThrow(
        /Condition "has_rows" of Dag "d" returned .* rather than a boolean/,
      );
    });
  });

  it("carries edges of its own, like any task", () => {
    const { dag, ifReady, gated } = gatedDag();
    const extracted = dag.task("extract", async () => undefined)();
    dag.if(gated).then(ifReady);

    gated.after(extracted);

    expect(getDagOrderEdges(dag)).toContainEqual({
      upstream: "extract",
      downstream: "has_rows",
    });
  });

  describe("rejects", () => {
    it("a condition taken from another Dag", () => {
      const { dag } = gatedDag("here");
      const { gated: foreign } = gatedDag("there");

      expect(() => dag.if(foreign)).toThrowError(
        /the condition given to dag.if cannot reach Dag "there" node "has_rows"/,
      );
    });

    it("a branch taken from another Dag", () => {
      const { dag, gated } = gatedDag("here");
      const { ifReady: foreign } = gatedDag("there");

      expect(() => dag.if(gated).then(foreign)).toThrowError(
        /the "then" branch of "has_rows" cannot reach Dag "there" node "load_if_ready"/,
      );
    });

    it("an else branch taken from another Dag", () => {
      const { dag, ifReady, gated } = gatedDag("here");
      const { fallback: foreign } = gatedDag("there");

      expect(() => dag.if(gated).then(ifReady).else(foreign)).toThrowError(
        /the "else" branch of "has_rows" cannot reach Dag "there"/,
      );
    });

    it.each([
      ["a plain object", { dagId: "d", taskId: "load_if_ready" }],
      ["a string", "load_if_ready"],
      ["null", null],
    ])("%s where a branch belongs", (_label, value) => {
      const { dag, gated } = gatedDag();

      expect(() => dag.if(gated).then(value as TaskRef)).toThrowError(
        /the "then" branch of "has_rows" on Dag "d" takes tasks and task groups/,
      );
    });

    it("both branches naming the same task, which decides nothing", () => {
      const { dag, ifReady, gated } = gatedDag();

      expect(() => dag.if(gated).then(ifReady).else(ifReady)).toThrowError(
        /Both branches of Dag "d" condition "has_rows" are "load_if_ready", so the condition decides nothing/,
      );
    });

    it("a second else branch", () => {
      const { dag, ifReady, fallback, gated } = gatedDag();
      const chain = dag.if(gated).then(ifReady);
      chain.else(fallback);

      expect(() => chain.else(fallback)).toThrowError(
        /Condition "has_rows" of Dag "d" already has an? "else" branch/,
      );
    });

    it("a task made a condition twice", () => {
      const { dag, ifReady, fallback, gated } = gatedDag();
      dag.if(gated).then(ifReady);

      expect(() => dag.if(gated).then(fallback)).toThrowError(
        /Task "has_rows" of Dag "d" already decides a branch/,
      );
    });

    it("a condition that names no branch, when the Dag is read", () => {
      const { dag, gated } = gatedDag();
      dag.if(gated);

      expect(() => finalizeDag(dag)).toThrowError(
        /Condition "has_rows" of Dag "d" names no branch, so it decides nothing/,
      );
    });

    it("being awaited, which would resolve the chain rather than build it", async () => {
      // An object with a callable `then` is a thenable, so `await` hands it a
      // resolve function where a task reference belongs.
      const { dag, ifReady, gated } = gatedDag();
      dag.task("other", async () => undefined)();

      await expect(Promise.resolve(dag.if(gated) as unknown as Promise<void>)).rejects.toThrow(
        /dag.if\(\.\.\.\) of Dag "d" was awaited/,
      );
      void ifReady;
    });
  });
});
