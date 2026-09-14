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
import { Dag } from "../../src/sdk/dag.js";
import { Bundle, listBundleDags, listBundleTasks } from "../../src/sdk/bundle.js";

describe("Bundle", () => {
  it("registers a Dag and retrieves its handlers", () => {
    const handler = async () => "hello";
    const dag = new Dag("example_dag");
    dag.task("my_task", handler);
    const bundle = new Bundle();
    bundle.register(dag);
    expect(bundle.getTaskHandler("example_dag", "my_task")).toBe(handler);
  });

  it("registers the Dags passed to its constructor", () => {
    const handler = async () => "hello";
    const dagA = new Dag("dag_a");
    dagA.task("a", handler);
    const bundle = new Bundle(dagA, new Dag("dag_b"));
    expect(bundle.getTaskHandler("dag_a", "a")).toBe(handler);
    expect(listBundleDags(bundle)).toEqual([
      { dagId: "dag_a", tasks: ["a"] },
      { dagId: "dag_b", tasks: [] },
    ]);
  });

  it("rejects duplicate dagIds passed to the constructor", () => {
    expect(() => new Bundle(new Dag("example_dag"), new Dag("example_dag"))).toThrowError(
      /already registered/,
    );
  });

  it("rejects constructor values that are not Dag instances", () => {
    expect(() => new Bundle({ dagId: "example_dag" } as unknown as Dag)).toThrowError(
      /only Dag instances can be registered/,
    );
  });

  it("returns undefined for unknown taskIds and dagIds", () => {
    const bundle = new Bundle();
    const dag = new Dag("example_dag");
    dag.task("my_task", async () => undefined);
    bundle.register(dag);
    expect(bundle.getTaskHandler("example_dag", "nope")).toBeUndefined();
    expect(bundle.getTaskHandler("unknown_dag", "my_task")).toBeUndefined();
  });

  it("returns an empty list when no Dags are registered", () => {
    const bundle = new Bundle();
    expect(listBundleTasks(bundle)).toEqual([]);
  });

  it("lists tasks across registered Dags", () => {
    const dagA = new Dag("dag_a");
    dagA.task("a", async () => undefined);
    const dagB = new Dag("dag_b");
    dagB.task("b", async () => undefined);
    const bundle = new Bundle();
    bundle.register(dagA, dagB);
    const registered = listBundleTasks(bundle);
    expect(registered).toHaveLength(2);
    expect(registered).toContainEqual({ dagId: "dag_a", taskId: "a" });
    expect(registered).toContainEqual({ dagId: "dag_b", taskId: "b" });
  });

  it("rejects registering the same dagId in separate calls", () => {
    const bundle = new Bundle();
    bundle.register(new Dag("example_dag"));
    expect(() => bundle.register(new Dag("example_dag"))).toThrowError(/already registered/);
  });

  it("rejects duplicate dagIds within a single call", () => {
    const bundle = new Bundle();
    expect(() => bundle.register(new Dag("example_dag"), new Dag("example_dag"))).toThrowError(
      /already registered/,
    );
  });

  it("rejects registering the same Dag instance twice", () => {
    const bundle = new Bundle();
    const dag = new Dag("example_dag");
    bundle.register(dag);
    expect(() => bundle.register(dag)).toThrowError(/already registered/);
  });

  it("registers none of the Dags when a call throws", () => {
    const bundle = new Bundle();
    const dag = new Dag("dag_a");
    dag.task("a", async () => undefined);
    expect(() => bundle.register(dag, new Dag("dag_a"))).toThrowError(/already registered/);
    expect(bundle.getTaskHandler("dag_a", "a")).toBeUndefined();
    expect(listBundleTasks(bundle)).toEqual([]);
  });

  it("rejects values that are not Dag instances", () => {
    const bundle = new Bundle();
    expect(() => bundle.register({ dagId: "example_dag" } as unknown as Dag)).toThrowError(
      /only Dag instances can be registered/,
    );
  });

  it("names the duplicate-copy cause when a Dag carries the brand but not this class", () => {
    // Stands in for a Dag from a second resolved copy: same brand, other class.
    const foreign = { dagId: "foreign_dag" };
    Object.defineProperty(foreign, Symbol.for("airflow.ts-sdk.Dag"), { value: true });
    expect(() => new Bundle(foreign as unknown as Dag)).toThrowError(
      /different copy of apache-airflow-ts-sdk/,
    );
  });

  it("lists every registered Dag with its tasks, empty Dags included", () => {
    const dagA = new Dag("dag_a");
    dagA.task("a1", async () => undefined);
    dagA.task("a2", async () => undefined);
    const bundle = new Bundle();
    bundle.register(dagA, new Dag("empty_dag"));
    expect(listBundleDags(bundle)).toEqual([
      { dagId: "dag_a", tasks: ["a1", "a2"] },
      { dagId: "empty_dag", tasks: [] },
    ]);
  });

  it("accepts a call that registers nothing", () => {
    // `bundle.register(...maybeDags)` with an empty list should not need a
    // guard at the call site.
    const bundle = new Bundle();
    expect(() => bundle.register()).not.toThrow();
    expect(listBundleDags(bundle)).toEqual([]);
  });

  it("carries the brand its own serve guard reads", () => {
    // The brand is how a bundle from a second resolved copy is told apart from
    // a plain object; `serve()` reports the two differently.
    expect(Symbol.for("airflow.ts-sdk.Bundle") in new Bundle()).toBe(true);
  });

  it("sees tasks added to a Dag after registration", () => {
    const bundle = new Bundle();
    const dag = new Dag("example_dag");
    bundle.register(dag);
    expect(listBundleTasks(bundle)).toEqual([]);

    const handler = async () => "late";
    dag.task("late_task", handler);
    expect(bundle.getTaskHandler("example_dag", "late_task")).toBe(handler);
    expect(listBundleTasks(bundle)).toContainEqual({
      dagId: "example_dag",
      taskId: "late_task",
    });
  });
});
