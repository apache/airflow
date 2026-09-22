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

import { Bundle, listBundleDags, listBundleTasks } from "../../src/sdk/bundle.js";
import { Dag } from "../../src/sdk/dag.js";
import { getTaskHandlerFunction, TaskHandler } from "../../src/sdk/task-handler.js";

describe("TaskHandler", () => {
  it("binds a function to the Python-owned task it implements", () => {
    const transform = async () => "transformed";
    const handler = new TaskHandler("etl", "transform", transform);

    expect(handler.dagId).toBe("etl");
    expect(handler.taskId).toBe("transform");
    expect(getTaskHandlerFunction(handler)).toBe(transform);
  });

  it("derives nothing from the function's name", () => {
    // The build step is free to rename or inline a function, so the task_id is
    // always written out and an anonymous handler is perfectly ordinary.
    const handler = new TaskHandler("etl", "transform", async () => undefined);
    expect(handler.taskId).toBe("transform");
  });

  it.each([
    ["an empty dagId", "", "transform", /dagId for a task handler must be a non-empty string/],
    ["an empty taskId", "etl", "", /taskId for a task handler must be a non-empty string/],
  ])("rejects %s", (_label, dagId, taskId, message) => {
    expect(() => new TaskHandler(dagId, taskId, async () => undefined)).toThrowError(message);
  });

  it("rejects a handler that is not a function", () => {
    expect(
      () => new TaskHandler("etl", "transform", "not a function" as unknown as () => void),
    ).toThrowError(/handler for Dag "etl" task "transform" must be a function/);
  });

  it("does not expose the function it carries", () => {
    // As with TaskRef: what a handler binds is identity. Reaching the body is
    // the runtime's business, through an accessor the package root never ships.
    const handler = new TaskHandler("etl", "transform", async () => undefined);
    for (const name of ["handler", "fn", "run", "call"]) {
      expect(name in handler).toBe(false);
    }
  });
});

describe("a bundle of task handlers", () => {
  it("registers Dags and task handlers in one call", () => {
    const nativeDag = new Dag("native_etl");
    nativeDag.task("extract", async () => undefined);
    const transform = async () => "transformed";

    const bundle = new Bundle();
    bundle.register(nativeDag, new TaskHandler("py_etl", "transform", transform));

    expect(bundle.getTaskHandler("py_etl", "transform")).toBe(transform);
    expect(bundle.getTaskHandler("native_etl", "extract")).toBeDefined();
  });

  it("dispatches on the Dag/task pair, not the task ID alone", () => {
    // The property the flattened map can get wrong and the old per-Dag map
    // could not: one bundle serves several Dags, and the same task_id under
    // two of them is two different handlers.
    const first = async () => "from etl";
    const second = async () => "from reporting";

    const bundle = new Bundle(
      new TaskHandler("etl", "build_message", first),
      new TaskHandler("reporting", "build_message", second),
    );

    expect(bundle.getTaskHandler("etl", "build_message")).toBe(first);
    expect(bundle.getTaskHandler("reporting", "build_message")).toBe(second);
    expect(bundle.getTaskHandler("unknown", "build_message")).toBeUndefined();
    expect(bundle.getTaskHandler("etl", "unknown")).toBeUndefined();
  });

  it("lists every Dag it provides for, in registration order", () => {
    const nativeDag = new Dag("native_etl");
    nativeDag.task("extract", async () => undefined);

    const bundle = new Bundle(
      new TaskHandler("py_etl", "transform", async () => undefined),
      nativeDag,
      new TaskHandler("py_etl", "report", async () => undefined),
    );

    // A Dag registered through handlers keeps its place from the first handler
    // that named it, so a later one does not reorder the manifest.
    expect(listBundleDags(bundle)).toEqual([
      { dagId: "py_etl", tasks: ["transform", "report"] },
      { dagId: "native_etl", tasks: ["extract"] },
    ]);
    expect(listBundleTasks(bundle)).toEqual([
      { dagId: "py_etl", taskId: "transform" },
      { dagId: "py_etl", taskId: "report" },
      { dagId: "native_etl", taskId: "extract" },
    ]);
  });

  it("accumulates handlers for one Dag across several calls", () => {
    const bundle = new Bundle();
    bundle.register(new TaskHandler("py_etl", "transform", async () => undefined));
    bundle.register(new TaskHandler("py_etl", "report", async () => undefined));

    expect(listBundleDags(bundle)).toEqual([{ dagId: "py_etl", tasks: ["transform", "report"] }]);
  });

  it("rejects a second handler for the same Dag and task", () => {
    const bundle = new Bundle(new TaskHandler("etl", "transform", async () => undefined));
    expect(() =>
      bundle.register(new TaskHandler("etl", "transform", async () => undefined)),
    ).toThrowError(/A handler for Dag "etl" task "transform" is already registered/);
  });

  it("rejects a duplicate Dag and task within a single call", () => {
    expect(
      () =>
        new Bundle(
          new TaskHandler("etl", "transform", async () => undefined),
          new TaskHandler("etl", "transform", async () => undefined),
        ),
    ).toThrowError(/A handler for Dag "etl" task "transform" is already registered/);
  });

  it("registers none of its items when a call throws", () => {
    const bundle = new Bundle();
    expect(() =>
      bundle.register(
        new TaskHandler("etl", "transform", async () => undefined),
        new TaskHandler("etl", "transform", async () => undefined),
      ),
    ).toThrowError(/already registered/);
    expect(listBundleDags(bundle)).toEqual([]);
    expect(bundle.getTaskHandler("etl", "transform")).toBeUndefined();
  });

  it("rejects a task handler for a Dag declared in TypeScript", () => {
    // A native Dag attaches its tasks with dag.task(...), so a handler for the
    // same Dag ID would be a second, disagreeing source for its task list.
    const bundle = new Bundle(new Dag("native_etl"));
    expect(() =>
      bundle.register(new TaskHandler("native_etl", "transform", async () => undefined)),
    ).toThrowError(/is declared in TypeScript; attach its tasks with dag\.task/);
  });

  it("rejects a task handler for a Dag declared in TypeScript in the same call", () => {
    expect(
      () =>
        new Bundle(
          new Dag("native_etl"),
          new TaskHandler("native_etl", "transform", async () => undefined),
        ),
    ).toThrowError(/is declared in TypeScript; attach its tasks with dag\.task/);
  });

  it("rejects a Dag whose ID already has task handlers", () => {
    const bundle = new Bundle(new TaskHandler("py_etl", "transform", async () => undefined));
    expect(() => bundle.register(new Dag("py_etl"))).toThrowError(
      /already has registered task handlers/,
    );
  });

  it("names the duplicate-copy cause for a handler carrying the brand but not this class", () => {
    // Stands in for a handler from a second resolved copy: same brand, other
    // class. Its private function field is unreadable here, so the point is
    // only that it says why.
    const foreign = { dagId: "etl", taskId: "transform" };
    Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.TaskHandler"), { value: true });
    expect(() => new Bundle(foreign as unknown as TaskHandler)).toThrowError(
      /Task handler for Dag "etl" task "transform" comes from a different copy/,
    );
  });
});
