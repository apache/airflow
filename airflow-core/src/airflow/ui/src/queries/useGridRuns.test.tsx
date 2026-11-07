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
import { expect, it, vi } from "vitest";

import { useGridRuns } from "./useGridRuns";

const { mockUseGridServiceGetGridRuns } = vi.hoisted(() => ({
  mockUseGridServiceGetGridRuns: vi.fn((..._args: Array<unknown>) => ({ data: undefined })),
}));

vi.mock("react-router-dom", () => ({ useParams: () => ({ dagId: "test_dag" }) }));
vi.mock("openapi/queries", () => ({ useGridServiceGetGridRuns: mockUseGridServiceGetGridRuns }));
vi.mock("src/hooks/useAdvancedSearch", () => ({ useAdvancedSearchArg: () => ({}) }));
vi.mock("src/utils", () => ({
  isStatePending: (state?: string) => state === "running",
  useAutoRefresh: () => 3000,
}));

it("can observe a grid-runs query without starting another polling loop", () => {
  renderHook(() => useGridRuns({ limit: 11, offset: 0 }, { autoRefresh: false, enabled: false }));

  const options = mockUseGridServiceGetGridRuns.mock.calls[0]?.[2] as
    | {
        enabled: boolean;
        refetchInterval: (query: { state: { data: Array<{ state: string }> } }) => number | false;
      }
    | undefined;

  expect(options?.enabled).toBe(false);
  expect(options?.refetchInterval({ state: { data: [{ state: "running" }] } })).toBe(false);
});
