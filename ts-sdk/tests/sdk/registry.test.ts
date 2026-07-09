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
import { TaskRegistry } from "../../src/sdk/registry.js";

describe("registry", () => {
  it("registers and retrieves a handler", async () => {
    const registry = new TaskRegistry();
    const handler = async () => "hello";
    registry.register({ dagId: "example_dag", taskId: "my_task" }, handler);
    const got = registry.get("example_dag", "my_task");
    expect(got).toBe(handler);
  });

  it("returns undefined for unknown taskIds", () => {
    const registry = new TaskRegistry();
    expect(registry.get("example_dag", "nope")).toBeUndefined();
    expect(registry.get("unknown_dag", "my_task")).toBeUndefined();
  });

  it("returns an empty list when no tasks are registered", () => {
    const registry = new TaskRegistry();
    expect(registry.list()).toEqual([]);
  });

  it("lists registered tasks", () => {
    const registry = new TaskRegistry();
    registry.register({ dagId: "dag_a", taskId: "a" }, async () => undefined);
    registry.register({ dagId: "dag_b", taskId: "b" }, async () => undefined);
    const registered = registry.list();
    expect(registered).toHaveLength(2);
    expect(registered).toContainEqual({ dagId: "dag_a", taskId: "a" });
    expect(registered).toContainEqual({ dagId: "dag_b", taskId: "b" });
  });

  it("rejects duplicate registration within a Dag", () => {
    const registry = new TaskRegistry();
    registry.register({ dagId: "example_dag", taskId: "dup" }, async () => undefined);
    expect(() =>
      registry.register({ dagId: "example_dag", taskId: "dup" }, async () => undefined),
    ).toThrowError(/already registered/);
  });

  it("allows the same taskId in different Dags", () => {
    const registry = new TaskRegistry();
    const first = async () => "first";
    const second = async () => "second";
    registry.register({ dagId: "first_dag", taskId: "extract" }, first);
    registry.register({ dagId: "second_dag", taskId: "extract" }, second);

    expect(registry.get("first_dag", "extract")).toBe(first);
    expect(registry.get("second_dag", "extract")).toBe(second);
  });

  it("rejects an empty dagId", () => {
    const registry = new TaskRegistry();
    expect(() =>
      registry.register({ dagId: "", taskId: "my_task" }, async () => undefined),
    ).toThrowError(/dagId must be made of alphanumeric/);
  });

  it("rejects an empty taskId", () => {
    const registry = new TaskRegistry();
    expect(() =>
      registry.register({ dagId: "example_dag", taskId: "" }, async () => undefined),
    ).toThrowError(/taskId must be made of alphanumeric/);
  });

  it.each(["   ", "\t", "my dag", "a/b", "task@1"])(
    "rejects a dagId with characters no Python dag_id allows: %j",
    (dagId) => {
      const registry = new TaskRegistry();
      expect(() =>
        registry.register({ dagId, taskId: "my_task" }, async () => undefined),
      ).toThrowError(/dagId must be made of alphanumeric/);
    },
  );

  it.each(["   ", "\t", "my task", "a/b", "task@1"])(
    "rejects a taskId with characters no Python task_id allows: %j",
    (taskId) => {
      const registry = new TaskRegistry();
      expect(() =>
        registry.register({ dagId: "example_dag", taskId }, async () => undefined),
      ).toThrowError(/taskId must be made of alphanumeric/);
    },
  );

  it.each([
    ["dagId", { dagId: "d".repeat(251), taskId: "my_task" }],
    ["taskId", { dagId: "example_dag", taskId: "t".repeat(251) }],
  ])("rejects a %s longer than 250 characters", (name, registration) => {
    const registry = new TaskRegistry();
    expect(() => registry.register(registration, async () => undefined)).toThrowError(
      new RegExp(`${name} must be less than 250 characters, not 251`),
    );
  });

  it("accepts a Unicode dagId that Python's word-character rule allows", () => {
    const registry = new TaskRegistry();
    const handler = async () => undefined;
    registry.register({ dagId: "café_dag", taskId: "任務" }, handler);
    expect(registry.get("café_dag", "任務")).toBe(handler);
  });

  it("rejects non-function handlers", () => {
    const registry = new TaskRegistry();
    expect(() =>
      registry.register(
        { dagId: "example_dag", taskId: "x" },
        "not a function" as unknown as () => Promise<unknown>,
      ),
    ).toThrowError(/must be a function/);
  });

  it("treats a dotted TaskGroup taskId as a single taskId (group.task)", () => {
    const registry = new TaskRegistry();
    registry.register({ dagId: "example_dag", taskId: "transforms.normalize" }, async () => "ok");
    expect(registry.get("example_dag", "transforms.normalize")).toBeDefined();
    // Should NOT accidentally match the prefix alone
    expect(registry.get("example_dag", "transforms")).toBeUndefined();
    expect(registry.get("example_dag", "normalize")).toBeUndefined();
  });
});
