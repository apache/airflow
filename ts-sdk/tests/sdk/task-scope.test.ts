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

import type { TaskClient } from "../../src/sdk/client.js";
import {
  getClient,
  getContext,
  runInTaskScope,
  type TaskContext,
  type TaskScope,
} from "../../src/sdk/task.js";

function makeScope(taskId: string): TaskScope {
  const ctx: TaskContext = {
    dagId: "scope_dag",
    taskId,
    runId: "r1",
    tryNumber: 1,
    mapIndex: -1,
    signal: new AbortController().signal,
  };
  // Identity is all these tests read, so a cast beats a full fake client.
  const client = { label: taskId } as unknown as TaskClient;
  return { ctx, client };
}

describe("the task scope", () => {
  it("hands a handler the context and client the runtime installed", async () => {
    const scope = makeScope("dispatched");

    const seen = await runInTaskScope(scope, async () => ({
      ctx: getContext(),
      client: getClient(),
    }));

    expect(seen.ctx).toBe(scope.ctx);
    expect(seen.client).toBe(scope.client);
  });

  it("survives await boundaries and reaches helpers it was never passed", async () => {
    // The point of AsyncLocalStorage over a parameter: a helper several frames
    // and several awaits deep reads the scope without anything being threaded.
    async function readTaskIdDeep(): Promise<string> {
      await Promise.resolve();
      await new Promise((resolve) => setTimeout(resolve, 0));
      return getContext().taskId;
    }

    const taskId = await runInTaskScope(makeScope("deep"), async () => {
      await Promise.resolve();
      await new Promise((resolve) => setImmediate(resolve));
      return await readTaskIdDeep();
    });

    expect(taskId).toBe("deep");
  });

  it("keeps concurrent handlers from seeing each other's scope", async () => {
    // A module variable would pass every single-handler test above, so this
    // is what pins the store as per-call.
    const run = (taskId: string, delayMs: number) =>
      runInTaskScope(makeScope(taskId), async () => {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        return getContext().taskId;
      });

    // The slower one is started first, so a shared store would answer "second".
    expect(await Promise.all([run("first", 10), run("second", 0)])).toEqual(["first", "second"]);
  });

  it("throws outside a handler, naming the accessor", () => {
    expect(() => getContext()).toThrow(/^getContext\(\) is only available inside a task handler/);
    expect(() => getClient()).toThrow(/^getClient\(\) is only available inside a task handler/);
  });

  it("throws in work started outside the handler it belongs to", async () => {
    // A callback handed to a module-level queue or an emitter registered at
    // import time does not carry the scope with it.
    let escaped: (() => TaskContext) | null = null;
    await runInTaskScope(makeScope("escaping"), async () => {
      escaped = () => getContext();
    });

    expect(escaped).not.toBeNull();
    expect(() => escaped!()).toThrow(/only available inside a task handler/);
  });

  it("still resolves in a floating promise, which is why one must be awaited", async () => {
    // By the time a promise the handler never awaited resolves, the runtime
    // has already reported the task's terminal state. Pinned because the fix
    // is to await the work, not to expect a throw here.
    let floating: Promise<string> | null = null;
    await runInTaskScope(makeScope("floating"), async () => {
      floating = (async () => {
        await new Promise((resolve) => setTimeout(resolve, 0));
        return getContext().taskId;
      })();
    });

    await expect(floating!).resolves.toBe("floating");
  });

  it("restores an outer scope after a nested one returns", async () => {
    const outer = makeScope("outer");

    const seen = await runInTaskScope(outer, async () => {
      const inner = await runInTaskScope(makeScope("inner"), async () => getContext().taskId);
      return { inner, afterInner: getContext().taskId };
    });

    expect(seen).toEqual({ inner: "inner", afterInner: "outer" });
  });

  it("is keyed globally, so a second resolved copy shares one storage", async () => {
    // Two copies of the package each import their own module instance. The
    // storage lives on globalThis under a well-known symbol so the copy that
    // dispatches and the copy a handler imported `getClient` from agree.
    const key = Symbol.for("airflow.ts-sdk.task-scope");
    const holder = globalThis as unknown as Record<symbol, unknown>;

    await runInTaskScope(makeScope("global"), async () => {
      expect(holder[key]).toBeDefined();
      expect(getContext().taskId).toBe("global");
    });
  });

  it("does not swallow a handler's own error", async () => {
    const boom = new Error("handler blew up");
    await expect(
      runInTaskScope(makeScope("failing"), async () => {
        throw boom;
      }),
    ).rejects.toBe(boom);
    // The scope unwinds with the throw rather than leaking to the next call.
    expect(() => getContext()).toThrow();
  });

  it("does not hold a reference the handler can reassign", async () => {
    const scope = makeScope("frozen");
    const spy = vi.fn();

    await runInTaskScope(scope, async () => {
      const first = getClient();
      const second = getClient();
      // Same object every read: nothing rebuilds a client per access.
      expect(first).toBe(second);
      spy();
    });

    expect(spy).toHaveBeenCalledOnce();
  });
});
