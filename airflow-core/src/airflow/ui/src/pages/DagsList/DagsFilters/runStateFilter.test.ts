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

import type { RunStateScope } from "./RunStateScopeSelect";
import { runScopeFor, runStateSelectionFor } from "./runStateFilter";

describe("runStateSelectionFor", () => {
  it("clears every param when no state is selected", () => {
    expect(runStateSelectionFor(undefined, "latest")).toEqual({});
    expect(runStateSelectionFor(undefined, "168")).toEqual({});
  });

  it("maps the latest scope to last_dag_run_state only", () => {
    expect(runStateSelectionFor("failed", "latest")).toEqual({ lastDagRunState: "failed" });
  });

  it("maps a time window to dag_run_state plus the within bound", () => {
    expect(runStateSelectionFor("failed", "168")).toEqual({
      dagRunState: "failed",
      dagRunStateWithinHours: "168",
    });
  });

  it("maps the any-time scope to dag_run_state without a window", () => {
    expect(runStateSelectionFor("success", "any")).toEqual({ dagRunState: "success" });
  });
});

describe("runScopeFor", () => {
  it.each<{
    any: string | null;
    expected: RunStateScope;
    last: string | null;
    window: string | null;
  }>([
    // last_dag_run_state present → latest scope, regardless of the others
    { any: null, expected: "latest", last: "failed", window: null },
    { any: "success", expected: "latest", last: "failed", window: "168" },
    // no state at all → default latest (control shows nothing until a state is picked)
    { any: null, expected: "latest", last: null, window: null },
    // any-run state with a window → that window
    { any: "failed", expected: "168", last: null, window: "168" },
    { any: "failed", expected: "24", last: null, window: "24" },
    // any-run state without a window → any-time
    { any: "failed", expected: "any", last: null, window: null },
  ])("last=$last any=$any window=$window resolves to $expected", ({ any, expected, last, window }) => {
    expect(runScopeFor(last, any, window)).toBe(expected);
  });
});
