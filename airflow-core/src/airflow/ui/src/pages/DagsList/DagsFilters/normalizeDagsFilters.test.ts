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

import { getNormalizedDagsFilterSearchParams } from "./normalizeDagsFilters";

describe("getNormalizedDagsFilterSearchParams", () => {
  it("drops invalid criteria, collapses duplicates, and preserves unknown parameters", () => {
    const params = new URLSearchParams(
      "last_dag_run_state=unknown&dag_run_state=failed&dag_run_state=success&paused=maybe" +
        "&needs_review=false&favorite=false&owners=airflow&owners=airflow&tags=a&tags=a&tags=&tags_match_mode=invalid" +
        "&timetable_type=CronTriggerTimetable&timetable_type=CronTriggerTimetable&future_filter=kept",
    );

    const normalized = getNormalizedDagsFilterSearchParams(params);

    expect(normalized.get("last_dag_run_state")).toBeNull();
    expect(normalized.getAll("dag_run_state")).toEqual(["failed"]);
    expect(normalized.get("paused")).toBeNull();
    expect(normalized.get("needs_review")).toBeNull();
    expect(normalized.get("favorite")).toBe("false");
    expect(normalized.getAll("tags")).toEqual(["a"]);
    expect(normalized.getAll("owners")).toEqual(["airflow"]);
    expect(normalized.get("tags_match_mode")).toBeNull();
    expect(normalized.getAll("timetable_type")).toEqual(["CronTriggerTimetable"]);
    expect(normalized.get("future_filter")).toBe("kept");
  });

  it("keeps the first valid repeated value and a valid tag mode", () => {
    const normalized = getNormalizedDagsFilterSearchParams(
      new URLSearchParams("last_dag_run_state=nope&last_dag_run_state=success&tags=a&tags_match_mode=all"),
    );

    expect(normalized.getAll("last_dag_run_state")).toEqual(["success"]);
    expect(normalized.get("tags_match_mode")).toBe("all");
  });

  it("drops no-op all values while preserving the meaningful paused override", () => {
    const normalized = getNormalizedDagsFilterSearchParams(
      new URLSearchParams("favorite=all&needs_review=all&paused=all"),
    );

    expect(normalized.get("favorite")).toBeNull();
    expect(normalized.get("needs_review")).toBeNull();
    expect(normalized.get("paused")).toBe("all");
  });
});
