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
import type { TriggerDagRunData, TriggerDagRunResponse } from "openapi/requests/types.gen";

import type { DagRunTriggerParams } from "src/components/TriggerDag/types";

import { Wrapper } from "src/utils/Wrapper";

import { useTrigger } from "./useTrigger";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useDagRunServiceTriggerDagRun: vi.fn() };
});

const { useDagRunServiceTriggerDagRun, UseDagServiceGetDagKeyFn } = await import("openapi/queries");

const DAG_ID = "paused_dag";
const mutate = vi.fn();

const triggerParams: DagRunTriggerParams = {
  conf: "{}",
  dagRunId: "",
  dataIntervalEnd: "",
  dataIntervalMode: "auto",
  dataIntervalStart: "",
  logicalDate: "",
  note: "",
  partitionKey: undefined,
};

describe("useTrigger", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useDagRunServiceTriggerDagRun).mockReturnValue({
      isPending: false,
      mutate,
    } as unknown as ReturnType<typeof useDagRunServiceTriggerDagRun>);
  });

  it("sends the drain choice as drain_dag", () => {
    const { result } = renderHook(() => useTrigger({ dagId: DAG_ID, onSuccessConfirm: vi.fn() }), {
      wrapper: Wrapper,
    });

    result.current.triggerDagRun({ ...triggerParams, drainDag: true });

    expect(mutate).toHaveBeenCalledWith({
      dagId: DAG_ID,
      requestBody: expect.objectContaining({ drain_dag: true }) as unknown,
    });
  });

  it.each([true, false])("refreshes the Dag's scheduling state only when drain_dag=%s", async (drainDag) => {
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries").mockResolvedValue();

    renderHook(() => useTrigger({ dagId: DAG_ID, onSuccessConfirm: vi.fn() }), { wrapper: Wrapper });

    const onSuccess = vi.mocked(useDagRunServiceTriggerDagRun).mock.calls.at(-1)?.[0]?.onSuccess as
      ((dagRun: TriggerDagRunResponse, variables: TriggerDagRunData) => Promise<void>) | undefined;

    await onSuccess?.({ dag_id: DAG_ID, dag_run_id: "manual__run" } as TriggerDagRunResponse, {
      dagId: DAG_ID,
      requestBody: { drain_dag: drainDag, logical_date: null },
    });

    const dagQuery = { queryKey: UseDagServiceGetDagKeyFn({ dagId: DAG_ID }, [{ dagId: DAG_ID }]) };

    if (drainDag) {
      expect(invalidateSpy).toHaveBeenCalledWith(dagQuery);
    } else {
      expect(invalidateSpy).not.toHaveBeenCalledWith(dagQuery);
    }
    invalidateSpy.mockRestore();
  });
});
