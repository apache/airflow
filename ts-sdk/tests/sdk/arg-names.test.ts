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

import { describe, expect, expectTypeOf, it, vi } from "vitest";

import { getArgNames, withArgNames, type ArgNameMap } from "../../src/sdk/arg-names.js";
import type { TaskFunction } from "../../src/sdk/task.js";

interface ReportArgs {
  label: string;
  threshold: number;
}

describe("withArgNames", () => {
  it("carries the renames the runtime reads back", () => {
    const handler = async ({ label }: ReportArgs) => label;
    const wrapped = withArgNames({ label: "run_label" }, handler);

    expect([...getArgNames(wrapped)]).toEqual([["label", "run_label"]]);
  });

  it("calls the handler it wraps, and returns its value", async () => {
    const handler = vi.fn(async ({ label, threshold }: ReportArgs) => `${label}:${threshold}`);
    const wrapped = withArgNames({ label: "run_label" }, handler);

    await expect(wrapped({ label: "nightly", threshold: 0.75 })).resolves.toBe("nightly:0.75");
    expect(handler).toHaveBeenCalledWith({ label: "nightly", threshold: 0.75 });
  });

  it("keeps the handler's own type, so the wrapped value is still registrable", () => {
    const wrapped = withArgNames({ label: "run_label" }, async ({ label }: ReportArgs) => label);
    expectTypeOf(wrapped).toEqualTypeOf<TaskFunction<ReportArgs, string>>();
  });

  it("does not mutate the function it wraps", () => {
    // One handler can be registered for two tasks that rename differently, so
    // writing the map onto the author's function would let the second
    // registration silently change the first.
    const handler = async ({ label }: ReportArgs) => label;
    const first = withArgNames({ label: "run_label" }, handler);
    const second = withArgNames({ label: "report_label" }, handler);

    expect(getArgNames(handler).size).toBe(0);
    expect(getArgNames(first).get("label")).toBe("run_label");
    expect(getArgNames(second).get("label")).toBe("report_label");
  });

  it("reports no renames for a plain handler", () => {
    expect(getArgNames(async () => undefined).size).toBe(0);
  });

  it("rejects a map that is not a plain object", () => {
    // Typed, so plain JavaScript is what these catch.
    const handler = async () => undefined;
    for (const bad of [null, undefined, "run_label", ["run_label"], new Map()]) {
      expect(() => withArgNames(bad as never, handler)).toThrowError(
        /takes a plain object mapping argument names to wire names/,
      );
    }
  });

  it("rejects a wire name that is not a non-empty string", () => {
    const handler = async ({ label }: ReportArgs) => label;
    for (const bad of [1, "", null, {}]) {
      expect(() =>
        withArgNames({ label: bad } as unknown as ArgNameMap<ReportArgs>, handler),
      ).toThrowError(/a wire name must be a non-empty string/);
    }
  });

  it("rejects a second argument that is not a function", () => {
    expect(() =>
      withArgNames({ label: "run_label" }, "not a handler" as unknown as TaskFunction<ReportArgs>),
    ).toThrowError(/takes the handler function as its second argument/);
  });

  it("checks the map's keys against the handler's own parameter type", () => {
    // The whole point of the type: a mapping for a name the handler does not
    // have would silently do nothing, so it must not compile.
    const rejectsUnknownKeys = () => {
      // @ts-expect-error "labl" is not a parameter of ReportArgs; "label" is.
      withArgNames({ labl: "run_label" }, async ({ label }: ReportArgs) => label);
      // @ts-expect-error a wire name is a string, not a task reference.
      withArgNames({ label: 1 }, async ({ label }: ReportArgs) => label);
      // @ts-expect-error the mapping comes first, the handler second.
      withArgNames(async ({ label }: ReportArgs) => label, { label: "run_label" });
    };
    void rejectsUnknownKeys;

    // Every key optional, so a handler renames only what it needs to.
    expectTypeOf<ArgNameMap<ReportArgs>>().toEqualTypeOf<{
      readonly label?: string;
      readonly threshold?: string;
    }>();
  });
});
