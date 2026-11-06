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
import { QueryClient } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { useBulkClearDagRuns } from "./useBulkClearDagRuns";
import { useBulkPatchDagRun } from "./useBulkPatchDagRun";
import { useClearDagRun } from "./useClearRun";
import { useClearTaskInstances } from "./useClearTaskInstances";
import { useCreateBackfill } from "./useCreateBackfill";
import { usePatchDagRun } from "./usePatchDagRun";
import { useTrigger } from "./useTrigger";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useBackfillServiceCreateBackfill: vi.fn(),
    useDagRunServiceBulkDagRuns: vi.fn(),
    useDagRunServiceClearDagRun: vi.fn(),
    useDagRunServiceClearDagRuns: vi.fn(),
    useDagRunServicePatchDagRun: vi.fn(),
    useDagRunServiceTriggerDagRun: vi.fn(),
    useTaskInstanceServicePostClearTaskInstances: vi.fn(),
  };
});

const queries = await import("openapi/queries");

type OnSuccess = (...args: Array<unknown>) => Promise<void>;

const getOnSuccess = (generatedHook: unknown) =>
  vi.mocked(generatedHook as (options: { onSuccess: OnSuccess }) => unknown).mock.calls.at(-1)?.[0].onSuccess;

const dagRun = { dag_id: "dag", dag_run_id: "run" };

describe.each([
  {
    args: [dagRun, { dagId: "dag", requestBody: { logical_date: null } }],
    generatedHook: queries.useDagRunServiceTriggerDagRun,
    operation: "triggering a Dag",
    useMutation: () => useTrigger({ dagId: "dag", onSuccessConfirm: vi.fn() }),
  },
  {
    args: [{ dag_id: "dag" }, { requestBody: { drain_dag: false } }],
    generatedHook: queries.useBackfillServiceCreateBackfill,
    operation: "creating a backfill",
    useMutation: () => useCreateBackfill({ onSuccessConfirm: vi.fn() }),
  },
  {
    args: [],
    generatedHook: queries.useDagRunServicePatchDagRun,
    operation: "marking a run",
    useMutation: () => usePatchDagRun({ dagId: "dag", dagRunId: "run" }),
  },
  {
    args: [{ update: { errors: [], success: ["dag.run"] } }],
    generatedHook: queries.useDagRunServiceBulkDagRuns,
    operation: "marking runs in bulk",
    useMutation: () => useBulkPatchDagRun({ deselectKeys: vi.fn(), onSuccessConfirm: vi.fn() }),
  },
  {
    args: [],
    generatedHook: queries.useDagRunServiceClearDagRun,
    operation: "clearing a run",
    useMutation: () => useClearDagRun({ dagId: "dag", dagRunId: "run", onSuccessConfirm: vi.fn() }),
  },
  {
    args: [{ dag_runs: [dagRun], total_entries: 1 }],
    generatedHook: queries.useDagRunServiceClearDagRuns,
    operation: "clearing runs in bulk",
    useMutation: () => useBulkClearDagRuns({ deselectKeys: vi.fn(), onSuccessConfirm: vi.fn() }),
  },
  {
    args: [
      { task_instances: [], total_entries: 0 },
      { dagId: "dag", requestBody: { dag_run_id: "run" } },
    ],
    generatedHook: queries.useTaskInstanceServicePostClearTaskInstances,
    operation: "clearing task instances",
    useMutation: () => useClearTaskInstances({ dagRunId: "run", onSuccessConfirm: vi.fn() }),
  },
])("Dag cards after $operation", ({ args, generatedHook, useMutation }) => {
  beforeEach(() => {
    vi.mocked(generatedHook as () => unknown).mockReturnValue({
      isPending: false,
      mutate: vi.fn(),
      reset: vi.fn(),
    });
  });
  afterEach(() => vi.restoreAllMocks());

  it("invalidates the Dag list and its run and task state counts", async () => {
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries").mockResolvedValue();

    renderHook(
      () => {
        useMutation();
      },
      { wrapper: Wrapper },
    );
    await getOnSuccess(generatedHook)?.(...args);

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: [queries.useDagServiceGetDagsUiKey] });
    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: [queries.useDagServiceGetDagRunStateCountsUiKey],
    });
    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: [queries.useDagServiceGetRecentTaskInstanceStateCountsUiKey],
    });
  });
});
