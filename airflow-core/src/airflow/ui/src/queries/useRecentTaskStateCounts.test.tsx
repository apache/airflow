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
import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi, type Mock } from "vitest";

import { useDagServiceGetRecentTaskInstanceStateCountsUi } from "openapi/queries";
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { useRecentTaskStateCounts } from "./useRecentTaskStateCounts";

vi.mock("openapi/queries", () => ({
  useDagServiceGetRecentTaskInstanceStateCountsUi: vi.fn(() => ({ data: undefined, isLoading: false })),
}));

vi.mock("src/utils", () => ({
  useAutoRefresh: () => 3000,
}));

const makeDag = (overrides: Partial<DAGWithLatestDagRunsResponse>) =>
  ({
    dag_id: "my_dag",
    has_unfinished_runs: false,
    is_paused: false,
    // The latest run already finished; any unfinished run is an older one.
    latest_dag_runs: [{ id: 1, state: "success" }],
    ...overrides,
  }) as DAGWithLatestDagRunsResponse;

describe("useRecentTaskStateCounts", () => {
  it.each([
    { expected: 3000, has_unfinished_runs: true, is_paused: false },
    { expected: false, has_unfinished_runs: false, is_paused: false },
    { expected: false, has_unfinished_runs: true, is_paused: true },
  ])(
    "refetches every $expected when has_unfinished_runs=$has_unfinished_runs and is_paused=$is_paused",
    ({ expected, ...dag }) => {
      renderHook(() => useRecentTaskStateCounts([makeDag(dag)]));

      const [, , options] = (useDagServiceGetRecentTaskInstanceStateCountsUi as Mock).mock.lastCall as [
        unknown,
        unknown,
        { refetchInterval: number | false },
      ];

      expect(options.refetchInterval).toBe(expected);
    },
  );
});
