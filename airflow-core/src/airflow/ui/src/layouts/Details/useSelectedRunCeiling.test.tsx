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
import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { GridRunsResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { useSelectedRunCeiling } from "./useSelectedRunCeiling";

// What the dag_runs mock handler answers for any run: run_after of `dagRunInRange`.
const RUN_AFTER = "2025-01-15T00:00:00Z";

const latestPage = [{ run_id: "latest" }] as unknown as ReadonlyArray<GridRunsResponse>;

type HookProps = { readonly dagId: string; readonly runId: string | undefined };

const renderCeiling = (initialProps: HookProps) => {
  const setRunAfterLte = vi.fn<(value: string | undefined) => void>();
  const rendered = renderHook(
    ({ dagId, runId }: HookProps) =>
      useSelectedRunCeiling({
        dagId,
        initialGridRuns: latestPage,
        noDateFilters: true,
        runId,
        setRunAfterLte,
      }),
    { initialProps, wrapper: BaseWrapper },
  );

  return { ...rendered, setRunAfterLte };
};

// Gives a query the hook would wrongly enable the chance to resolve and set the ceiling.
const settle = () => act(() => new Promise((resolve) => setTimeout(resolve, 50)));

describe("useSelectedRunCeiling", () => {
  it("sets the ceiling for a run the latest page doesn't show", async () => {
    const { setRunAfterLte } = renderCeiling({ dagId: "dag_a", runId: "old_1" });

    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledWith(RUN_AFTER));
  });

  it("leaves the window alone when the selected run is on the latest page", async () => {
    const { setRunAfterLte } = renderCeiling({ dagId: "dag_a", runId: "latest" });

    await settle();
    expect(setRunAfterLte).not.toHaveBeenCalled();
  });

  it("doesn't shift the window when moving between runs at the run level", async () => {
    const { rerender, setRunAfterLte } = renderCeiling({ dagId: "dag_a", runId: "old_1" });

    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledTimes(1));
    rerender({ dagId: "dag_a", runId: "old_2" });
    await settle();
    expect(setRunAfterLte).toHaveBeenCalledTimes(1);
  });

  it("sets it again for the next off-grid run opened after returning to the Dag level", async () => {
    const { rerender, setRunAfterLte } = renderCeiling({ dagId: "dag_a", runId: "old_1" });

    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledTimes(1));
    rerender({ dagId: "dag_a", runId: undefined });
    rerender({ dagId: "dag_a", runId: "old_2" });
    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledTimes(2));
  });

  it("sets it again after switching to another Dag", async () => {
    const { rerender, setRunAfterLte } = renderCeiling({ dagId: "dag_a", runId: "old_1" });

    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledTimes(1));
    rerender({ dagId: "dag_b", runId: "old_1" });
    await waitFor(() => expect(setRunAfterLte).toHaveBeenCalledTimes(2));
  });
});
