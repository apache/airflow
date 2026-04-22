/* eslint-disable unicorn/no-null -- GridRunsResponse uses null for optional API fields */

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

import type { GridRunsResponse } from "openapi/requests";

import { computePaginationState } from "./useGridPagination";

/** Minimal GridRunsResponse stub with a given run_id. */
const makeRun = (runId: string): GridRunsResponse => ({
  dag_id: "my_dag",
  duration: 0,
  end_date: null,
  has_missed_deadline: false,
  queued_at: null,
  run_after: "2024-01-01T00:00:00Z",
  run_id: runId,
  run_type: "manual",
  start_date: null,
  state: "success",
});

/** Ten runs used to fill a full page in hasOlderRuns tests. */
const TEN_RUNS: Array<GridRunsResponse> = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((index) =>
  makeRun(`run_${index}`),
);

describe("computePaginationState", () => {
  describe("latestNotVisible", () => {
    it("is false when gridRuns is still loading (undefined)", () => {
      const { latestNotVisible } = computePaginationState({
        gridRuns: undefined,
        latestRunId: "run_1",
        limit: 10,
        offset: 0,
      });

      expect(latestNotVisible).toBe(false);
    });

    it("is false when latestRunId is not yet known (undefined)", () => {
      const { latestNotVisible } = computePaginationState({
        gridRuns: [makeRun("run_1")],
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(latestNotVisible).toBe(false);
    });

    it("is false when the latest run is present in the current page", () => {
      const { latestNotVisible } = computePaginationState({
        gridRuns: [makeRun("run_3"), makeRun("run_2"), makeRun("run_1")],
        latestRunId: "run_3",
        limit: 10,
        offset: 0,
      });

      expect(latestNotVisible).toBe(false);
    });

    it("is true when the latest run is absent from the current page", () => {
      const { latestNotVisible } = computePaginationState({
        gridRuns: [makeRun("run_2"), makeRun("run_1")],
        latestRunId: "run_3",
        limit: 10,
        offset: 0,
      });

      expect(latestNotVisible).toBe(true);
    });
  });

  describe("hasOlderRuns", () => {
    it("is false when the page is not full", () => {
      const { hasOlderRuns } = computePaginationState({
        gridRuns: [makeRun("run_1"), makeRun("run_2")],
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(hasOlderRuns).toBe(false);
    });

    it("is false when the page is exactly full", () => {
      const { hasOlderRuns } = computePaginationState({
        gridRuns: TEN_RUNS,
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(hasOlderRuns).toBe(false);
    });

    it("is false when gridRuns is undefined", () => {
      const { hasOlderRuns } = computePaginationState({
        gridRuns: undefined,
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(hasOlderRuns).toBe(false);
    });

    it("is true when gridRuns has more runs than limit", () => {
      const { hasOlderRuns } = computePaginationState({
        gridRuns: [...TEN_RUNS, makeRun("run_10")],
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(hasOlderRuns).toBe(true);
    });
  });

  describe("hasNewerRuns", () => {
    it("is false at offset 0 (already on the newest page)", () => {
      const { hasNewerRuns } = computePaginationState({
        gridRuns: [],
        latestRunId: undefined,
        limit: 10,
        offset: 0,
      });

      expect(hasNewerRuns).toBe(false);
    });

    it("is true when offset is greater than zero", () => {
      const { hasNewerRuns } = computePaginationState({
        gridRuns: [],
        latestRunId: undefined,
        limit: 10,
        offset: 10,
      });

      expect(hasNewerRuns).toBe(true);
    });
  });
});
