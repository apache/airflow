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

import { describe, it, expect, beforeEach } from "vitest";
import {
  registerTask,
  getRegisteredTask,
  listRegisteredTasks,
  clearRegistry,
} from "../src/registry.js";

describe("registry", () => {
  beforeEach(() => {
    clearRegistry();
  });

  it("registers and retrieves a handler", async () => {
    const handler = async () => "hello";
    registerTask("my_task", handler);
    const got = getRegisteredTask("my_task");
    expect(got).toBe(handler);
  });

  it("returns undefined for unknown task_ids", () => {
    expect(getRegisteredTask("nope")).toBeUndefined();
  });

  it("lists registered task_ids", () => {
    registerTask("a", async () => undefined);
    registerTask("b", async () => undefined);
    expect(listRegisteredTasks().sort()).toEqual(["a", "b"]);
  });

  it("rejects duplicate registration", () => {
    registerTask("dup", async () => undefined);
    expect(() => registerTask("dup", async () => undefined)).toThrowError(/already registered/);
  });

  it("rejects empty string task_id", () => {
    expect(() => registerTask("", async () => undefined)).toThrowError(/non-empty string/);
  });

  it("rejects non-function handlers", () => {
    expect(() =>
      registerTask("x", "not a function" as unknown as () => Promise<unknown>),
    ).toThrowError(/must be a function/);
  });

  it("handles dotted TaskGroup task_ids (group.task)", () => {
    registerTask("transforms.normalize", async () => "ok");
    expect(getRegisteredTask("transforms.normalize")).toBeDefined();
    // Should NOT accidentally match the prefix alone
    expect(getRegisteredTask("transforms")).toBeUndefined();
    expect(getRegisteredTask("normalize")).toBeUndefined();
  });

  it("clearRegistry empties everything", () => {
    registerTask("a", async () => undefined);
    registerTask("b", async () => undefined);
    expect(listRegisteredTasks().length).toBe(2);
    clearRegistry();
    expect(listRegisteredTasks()).toEqual([]);
  });
});
