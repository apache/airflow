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

/** A Dag with three placed tasks and a placed decider that returns one of them. */
function branchedDag(dagId = "d", choose?: (refs: Record<string, TaskRef>) => unknown) {
  const dag = new Dag(dagId);
  const long = dag.task("handle_long", async () => undefined)();
  const short = dag.task("handle_short", async () => undefined)();
  const other = dag.task("handle_other", async () => undefined)();
  const refs = { long, short, other };
  const decider = dag.task("pick_path", async () => (choose ? (choose(refs) as TaskRef) : long))();
  return { dag, long, short, other, decider };
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

describe("dag.switch", () => {
  it("leaves the decider an ordinary task of the Dag", () => {
    const { dag, long, short, decider } = branchedDag();
    dag.switch(decider).case(long).case(short);

    expect(dag.taskIds).toEqual(["handle_long", "handle_short", "handle_other", "pick_path"]);
  });

  it("marks the decider as deciding skips, which Airflow reads on a clear", () => {
    const { dag, long, short, decider } = branchedDag();
    dag.switch(decider).case(long).case(short);

    const tasks = (serializeDag(dag, "", ".") as { tasks: { __var: Record<string, unknown> }[] })
      .tasks;
    const byId = new Map(tasks.map(({ __var }) => [__var["task_id"] as string, __var]));
    expect(byId.get("pick_path")?.["_can_skip_downstream"]).toBe(true);
    expect(byId.get("handle_long")).not.toHaveProperty("_can_skip_downstream");
  });

  it("draws an order-only edge to each case, since no value flows", () => {
    const { dag, long, short, decider } = branchedDag();
    dag.switch(decider).case(long).case(short);

    expect(getDagOrderEdges(dag)).toEqual([
      { upstream: "pick_path", downstream: "handle_long" },
      { upstream: "pick_path", downstream: "handle_short" },
    ]);
  });

  it("takes a single case, which is then the only side that can run", () => {
    const { dag, long, decider } = branchedDag();
    dag.switch(decider).case(long);

    expect(getDagOrderEdges(dag)).toEqual([{ upstream: "pick_path", downstream: "handle_long" }]);
  });

  describe("at run time", () => {
    it("returns the chosen task's id, which is what a branch puts on the wire", async () => {
      const { dag, long, short, decider } = branchedDag("d", ({ short: s }) => s);
      dag.switch(decider).case(long).case(short);

      expect((await runHandler(dag, "pick_path")).returned).toBe("handle_short");
    });

    it("skips every case it did not choose", async () => {
      const { dag, long, short, other, decider } = branchedDag("d", ({ long: l }) => l);
      dag.switch(decider).case(long).case(short).case(other);

      const { skipDownstreamTasks, setXCom } = await runHandler(dag, "pick_path");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["handle_short", "handle_other"]);
      expect(setXCom).toHaveBeenCalledWith({
        key: "skipmixin_key",
        value: { skipped: ["handle_short", "handle_other"] },
      });
    });

    it("skips nothing when its one case is the one chosen", async () => {
      const { dag, long, decider } = branchedDag("d", ({ long: l }) => l);
      dag.switch(decider).case(long);

      const { skipDownstreamTasks } = await runHandler(dag, "pick_path");

      expect(skipDownstreamTasks).not.toHaveBeenCalled();
    });

    it("reads the cases at run time, so the chain can be written after", async () => {
      const { dag, long, short, decider } = branchedDag("d", ({ long: l }) => l);
      const chain = dag.switch(decider);
      chain.case(long).case(short);

      const { skipDownstreamTasks } = await runHandler(dag, "pick_path");

      expect(skipDownstreamTasks).toHaveBeenCalledWith(["handle_short"]);
    });

    it("fails the task when the decider chooses a task that is not a case", async () => {
      const { dag, long, short, decider } = branchedDag("d", ({ other }) => other);
      dag.switch(decider).case(long).case(short);

      await expect(runHandler(dag, "pick_path")).rejects.toThrow(
        /Task "pick_path" of Dag "d" chose "handle_other", which is not one of its cases: handle_long, handle_short/,
      );
    });

    it.each([
      ["a string", "handle_long"],
      ["a look-alike object", { dagId: "d", taskId: "handle_long" }],
      ["null", null],
    ])("fails the task when the decider returns %s", async (_label, value) => {
      const { dag, long, short, decider } = branchedDag("d", () => value);
      dag.switch(decider).case(long).case(short);

      await expect(runHandler(dag, "pick_path")).rejects.toThrow(/which is not one of its cases/);
    });

    it("skips nothing and fails when the decider chose wrongly", async () => {
      const { dag, long, short, decider } = branchedDag("d", ({ other }) => other);
      dag.switch(decider).case(long).case(short);

      const skipDownstreamTasks = vi.fn(async () => undefined);
      const client = { skipDownstreamTasks, setXCom: vi.fn() } as unknown as TaskClient;
      const ctx = { dagId: "d", taskId: "pick_path" } as unknown as TaskContext;
      const handler = getDagTaskRecords(dag).get("pick_path")!.fn;

      await expect(
        runInTaskScope({ ctx, client }, () => (handler as (args: unknown) => Promise<unknown>)({})),
      ).rejects.toThrow();
      expect(skipDownstreamTasks).not.toHaveBeenCalled();
    });
  });

  it("carries edges of its own, like any task", () => {
    const { dag, long, decider } = branchedDag();
    const extracted = dag.task("extract", async () => undefined)();
    dag.switch(decider).case(long);

    decider.after(extracted);

    expect(getDagOrderEdges(dag)).toContainEqual({
      upstream: "extract",
      downstream: "pick_path",
    });
  });

  describe("rejects", () => {
    it("a decider taken from another Dag", () => {
      const { dag } = branchedDag("here");
      const { decider: foreign } = branchedDag("there");

      expect(() => dag.switch(foreign)).toThrowError(
        /the condition given to dag.switch cannot reach Dag "there" node "pick_path"/,
      );
    });

    it("a case taken from another Dag", () => {
      const { dag, decider } = branchedDag("here");
      const { long: foreign } = branchedDag("there");

      expect(() => dag.switch(decider).case(foreign)).toThrowError(
        /a case of "pick_path" cannot reach Dag "there" node "handle_long"/,
      );
    });

    it.each([
      ["a plain object", { dagId: "d", taskId: "handle_long" }],
      ["a string", "handle_long"],
      ["null", null],
    ])("%s where a case belongs", (_label, value) => {
      const { dag, decider } = branchedDag();

      expect(() => dag.switch(decider).case(value as TaskRef)).toThrowError(
        /a case of "pick_path" on Dag "d" takes tasks and task groups/,
      );
    });

    it("the same task listed twice", () => {
      const { dag, long, decider } = branchedDag();

      expect(() => dag.switch(decider).case(long).case(long)).toThrowError(
        /Dag "d" branch "pick_path" lists "handle_long" twice; each case names a different task/,
      );
    });

    it("a task made a branch twice", () => {
      const { dag, long, short, decider } = branchedDag();
      dag.switch(decider).case(long);

      expect(() => dag.switch(decider).case(short)).toThrowError(
        /Task "pick_path" of Dag "d" already decides a branch/,
      );
    });

    it("a task that is both a condition and a branch", () => {
      const { dag, long, short, decider } = branchedDag();
      dag.switch(decider).case(long);

      expect(() => dag.if(decider as unknown as TaskRef<boolean>).then(short)).toThrowError(
        /Task "pick_path" of Dag "d" already decides a branch/,
      );
    });

    it("a branch with no case, when the Dag is read", () => {
      const { dag, decider } = branchedDag();
      dag.switch(decider);

      expect(() => finalizeDag(dag)).toThrowError(
        /Branch "pick_path" of Dag "d" has no cases, so it decides nothing/,
      );
    });
  });
});
