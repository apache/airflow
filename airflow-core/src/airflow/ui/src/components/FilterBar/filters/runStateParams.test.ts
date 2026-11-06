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

import { runStateFromSearchParams, runStateToSearchParams } from "./runStateParams";

describe("runStateToSearchParams", () => {
  it("maps the latest lookback onto the latest-run param only", () => {
    expect(runStateToSearchParams({ lookback: "latest", state: "failed" })).toEqual({
      dag_run_state: undefined,
      dag_run_state_within_hours: undefined,
      last_dag_run_state: "failed",
    });
  });

  it("maps the any lookback onto the any-run param without a time bound", () => {
    expect(runStateToSearchParams({ lookback: "any", state: "failed" })).toEqual({
      dag_run_state: "failed",
      dag_run_state_within_hours: undefined,
      last_dag_run_state: undefined,
    });
  });

  it("maps a time lookback onto the any-run param plus the within-hours bound", () => {
    expect(runStateToSearchParams({ lookback: "168", state: "failed" })).toEqual({
      dag_run_state: "failed",
      dag_run_state_within_hours: "168",
      last_dag_run_state: undefined,
    });
  });

  it.each([undefined, null, ""])("clears every managed param for %j", (value) => {
    expect(runStateToSearchParams(value)).toEqual({
      dag_run_state: undefined,
      dag_run_state_within_hours: undefined,
      last_dag_run_state: undefined,
    });
  });
});

describe("runStateFromSearchParams", () => {
  it("reads the latest-run param as the latest lookback", () => {
    expect(runStateFromSearchParams(new URLSearchParams("last_dag_run_state=failed"))).toEqual({
      lookback: "latest",
      state: "failed",
    });
  });

  it("reads the any-run param without a bound as the any lookback", () => {
    expect(runStateFromSearchParams(new URLSearchParams("dag_run_state=failed"))).toEqual({
      lookback: "any",
      state: "failed",
    });
  });

  it("reads a known within-hours bound as its time lookback", () => {
    expect(
      runStateFromSearchParams(new URLSearchParams("dag_run_state=failed&dag_run_state_within_hours=168")),
    ).toEqual({ lookback: "168", state: "failed" });
  });

  it.each(["48", "-5", "abc", ""])("falls back to the any lookback for unknown bound %j", (bound) => {
    expect(
      runStateFromSearchParams(
        new URLSearchParams({ dag_run_state: "failed", dag_run_state_within_hours: bound }),
      ),
    ).toEqual({ lookback: "any", state: "failed" });
  });

  it("prefers the latest-run param when both state params are present", () => {
    expect(
      runStateFromSearchParams(new URLSearchParams("last_dag_run_state=success&dag_run_state=failed")),
    ).toEqual({ lookback: "latest", state: "success" });
  });

  it("returns undefined when neither state param is present", () => {
    expect(runStateFromSearchParams(new URLSearchParams("dag_run_state_within_hours=24"))).toBeUndefined();
  });
});
