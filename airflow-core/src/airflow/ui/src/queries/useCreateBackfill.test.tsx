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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { BackfillResponse, CreateBackfillData } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { useCreateBackfill } from "./useCreateBackfill";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useBackfillServiceCreateBackfill: vi.fn() };
});

const {
  useBackfillServiceCreateBackfill,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  useDagServiceGetDagsUiKey,
} = await import("openapi/queries");

const DAG_ID = "paused_dag";
const mutate = vi.fn();

const buildRequest = (drainDag: boolean): CreateBackfillData => ({
  requestBody: {
    dag_id: DAG_ID,
    drain_dag: drainDag,
    from_date: "2024-01-01T00:00:00Z",
    to_date: "2024-01-02T00:00:00Z",
  },
});

describe("useCreateBackfill", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useBackfillServiceCreateBackfill).mockReturnValue({
      isPending: false,
      mutate,
    } as unknown as ReturnType<typeof useBackfillServiceCreateBackfill>);
  });

  it("sends the drain choice as drain_dag", () => {
    const { result } = renderHook(() => useCreateBackfill({ onSuccessConfirm: vi.fn() }), {
      wrapper: Wrapper,
    });

    result.current.createBackfill(buildRequest(true));

    expect(mutate).toHaveBeenCalledWith({
      requestBody: expect.objectContaining({ drain_dag: true }) as unknown,
    });
  });

  it.each([true, false])("refreshes the Dag's scheduling state only when drain_dag=%s", async (drainDag) => {
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries").mockResolvedValue();

    renderHook(() => useCreateBackfill({ onSuccessConfirm: vi.fn() }), { wrapper: Wrapper });

    const onSuccess = vi.mocked(useBackfillServiceCreateBackfill).mock.calls.at(-1)?.[0]?.onSuccess as
      ((backfill: BackfillResponse, variables: CreateBackfillData) => Promise<void>) | undefined;

    await onSuccess?.({ dag_id: DAG_ID } as BackfillResponse, buildRequest(drainDag));

    for (const queryKey of [
      UseDagServiceGetDagKeyFn({ dagId: DAG_ID }, [{ dagId: DAG_ID }]),
      UseDagServiceGetDagDetailsKeyFn({ dagId: DAG_ID }, [{ dagId: DAG_ID }]),
      [useDagServiceGetDagsUiKey],
    ]) {
      if (drainDag) {
        expect(invalidateSpy).toHaveBeenCalledWith({ queryKey });
      } else {
        expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey });
      }
    }
    invalidateSpy.mockRestore();
  });
});
