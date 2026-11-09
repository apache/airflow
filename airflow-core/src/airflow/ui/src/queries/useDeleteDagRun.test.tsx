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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  UseDagServiceGetDagRunStateCountsUiKeyFn,
  UseDagServiceGetDagsUiKeyFn,
  UseDagServiceGetRecentTaskInstanceStateCountsUiKeyFn,
} from "openapi/queries";
import { DagRunService } from "openapi/requests";

import { useBulkDeleteDagRuns } from "./useBulkDeleteDagRuns";
import { useDeleteDagRun } from "./useDeleteDagRun";

const useDeleteSingleRun = (onSuccessConfirm: VoidFunction) => {
  const { mutate } = useDeleteDagRun({ dagId: "dag", dagRunId: "run", onSuccessConfirm });

  return () => mutate({ dagId: "dag", dagRunId: "run" });
};

const useDeleteMultipleRuns = (onSuccessConfirm: VoidFunction) => {
  const { bulkAction } = useBulkDeleteDagRuns({ deselectKeys: vi.fn(), onSuccessConfirm });

  return () =>
    bulkAction({ actions: [{ action: "delete", entities: [{ dag_id: "dag", dag_run_id: "run" }] }] });
};

describe.each([
  { operation: "single-run deletion", useDelete: useDeleteSingleRun },
  { operation: "bulk-run deletion", useDelete: useDeleteMultipleRuns },
])("Dag card cache after $operation", ({ useDelete }) => {
  beforeEach(() => {
    vi.spyOn(DagRunService, "deleteDagRun").mockResolvedValue(undefined);
    vi.spyOn(DagRunService, "bulkDagRuns").mockResolvedValue({
      delete: { errors: [], success: ["dag.run"] },
    });
  });
  afterEach(() => vi.restoreAllMocks());

  it.each([
    { name: "Dag list", queryKey: UseDagServiceGetDagsUiKeyFn({ limit: 50 }) },
    { name: "run-state counts", queryKey: UseDagServiceGetDagRunStateCountsUiKeyFn({ dagIds: ["dag"] }) },
    {
      name: "recent task-state counts",
      queryKey: UseDagServiceGetRecentTaskInstanceStateCountsUiKeyFn({ dagRunIds: [1] }),
    },
  ])("invalidates the cached $name before navigating back", async ({ queryKey }) => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity } } });

    queryClient.setQueryData(queryKey, { cached: true });
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const onSuccessConfirm = vi.fn();
    const { result } = renderHook(() => useDelete(onSuccessConfirm), { wrapper });

    act(() => result.current());
    await waitFor(() => expect(onSuccessConfirm).toHaveBeenCalled());
    expect(queryClient.getQueryState(queryKey)?.isInvalidated).toBe(true);
    queryClient.clear();
  });
});
