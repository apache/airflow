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
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { useRefreshOnNewDagRuns } from "./useRefreshOnNewDagRuns";
import * as openApiQueries from "../openapi/queries";
import * as useConfigModule from "./useConfig";

// Mock the dependencies
vi.mock("../openapi/queries", () => ({
  useDagServiceGetDagDetailsKey: "dag-details-key",
  UseDagRunServiceGetDagRunsKeyFn: vi.fn(() => ["dag-runs-key"]),
  UseDagServiceGetDagDetailsKeyFn: vi.fn(() => ["dag-details-fn-key"]),
  useDagServiceGetDagsUi: "dags-ui-key",
  UseTaskInstanceServiceGetTaskInstancesKeyFn: vi.fn(() => ["task-instances-key"]),
  UseGridServiceGetDagStructureKeyFn: vi.fn(() => ["grid-structure-key"]),
  UseGridServiceGetGridRunsKeyFn: vi.fn(() => ["grid-runs-key"]),
  useDagServiceGetLatestRunInfo: vi.fn(),
}));

vi.mock("./useConfig", () => ({
  useConfig: vi.fn(),
}));

describe("useRefreshOnNewDagRuns", () => {
  let queryClient: QueryClient;
  let wrapper: ({ children }: { children: ReactNode }) => JSX.Element;

  beforeEach(() => {
    // Create a new QueryClient for each test
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    // Create wrapper with QueryClientProvider
    wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    // Mock useConfig to return auto refresh interval
    vi.mocked(useConfigModule.useConfig).mockReturnValue(5);

    // Reset all mocks
    vi.clearAllMocks();
  });

  it("should not invalidate queries on initial load when latestDagRunId is undefined", () => {
    const dagId = "test_dag";
    const invalidateQueriesSpy = vi.spyOn(queryClient, "invalidateQueries");

    // Mock useDagServiceGetLatestRunInfo to return undefined initially
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Should not invalidate queries when data is undefined on initial load
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
  });

  it("should not invalidate queries on initial load when latestDagRunId is first fetched", () => {
    const dagId = "test_dag";
    const invalidateQueriesSpy = vi.spyOn(queryClient, "invalidateQueries");

    // Mock useDagServiceGetLatestRunInfo to return a run_id on first render
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: { run_id: "run_123" },
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Should not invalidate queries on initial load even when latestDagRunId is present
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
  });

  it("should invalidate queries when a new DAG run appears", async () => {
    const dagId = "test_dag";
    const invalidateQueriesSpy = vi.spyOn(queryClient, "invalidateQueries");

    // Start with initial run_id
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: { run_id: "run_123" },
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    const { rerender } = renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Initial render should not invalidate
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();

    // Update to a new run_id
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: { run_id: "run_456" },
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    rerender();

    // Should now invalidate queries because run_id changed
    await waitFor(() => {
      expect(invalidateQueriesSpy).toHaveBeenCalled();
    });

    // Verify all the expected query keys were invalidated
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["dags-ui-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["dag-details-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["dag-details-fn-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["dag-runs-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["task-instances-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["grid-structure-key"] });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ["grid-runs-key"] });
  });

  it("should not invalidate queries when latestDagRunId remains the same", () => {
    const dagId = "test_dag";
    const invalidateQueriesSpy = vi.spyOn(queryClient, "invalidateQueries");

    // Mock with same run_id
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: { run_id: "run_123" },
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    const { rerender } = renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Initial render - no invalidation
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();

    // Rerender with same run_id
    rerender();

    // Should still not invalidate because run_id hasn't changed
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
  });

  it("should handle transition from undefined to defined run_id without invalidation on first occurrence", async () => {
    const dagId = "test_dag";
    const invalidateQueriesSpy = vi.spyOn(queryClient, "invalidateQueries");

    // Start with undefined
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    const { rerender } = renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // No invalidation on initial undefined
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();

    // Update to have a run_id (simulating data being fetched)
    vi.mocked(openApiQueries.useDagServiceGetLatestRunInfo).mockReturnValue({
      data: { run_id: "run_123" },
    } as ReturnType<typeof openApiQueries.useDagServiceGetLatestRunInfo>);

    rerender();

    // Should not invalidate on first transition from undefined to defined
    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
  });

  it("should not fetch latest run info when hasPendingRuns is true", () => {
    const dagId = "test_dag";

    renderHook(() => useRefreshOnNewDagRuns(dagId, true), { wrapper });

    // Verify useDagServiceGetLatestRunInfo was called with enabled: false
    expect(openApiQueries.useDagServiceGetLatestRunInfo).toHaveBeenCalledWith(
      { dagId },
      undefined,
      expect.objectContaining({
        enabled: false,
      }),
    );
  });

  it("should use custom auto refresh interval from config", () => {
    const dagId = "test_dag";
    const customInterval = 10;

    vi.mocked(useConfigModule.useConfig).mockReturnValue(customInterval);

    renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Verify useDagServiceGetLatestRunInfo was called with custom interval
    expect(openApiQueries.useDagServiceGetLatestRunInfo).toHaveBeenCalledWith(
      { dagId },
      undefined,
      expect.objectContaining({
        refetchInterval: customInterval * 1000, // Should be in milliseconds
      }),
    );
  });

  it("should use default 5 second interval when auto_refresh_interval is not set", () => {
    const dagId = "test_dag";

    vi.mocked(useConfigModule.useConfig).mockReturnValue(0);

    renderHook(() => useRefreshOnNewDagRuns(dagId, false), { wrapper });

    // Verify useDagServiceGetLatestRunInfo was called with default 5000ms
    expect(openApiQueries.useDagServiceGetLatestRunInfo).toHaveBeenCalledWith(
      { dagId },
      undefined,
      expect.objectContaining({
        refetchInterval: 5000,
      }),
    );
  });
});
