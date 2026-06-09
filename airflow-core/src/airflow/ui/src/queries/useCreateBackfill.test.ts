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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useBackfillServiceCreateBackfill } from "openapi/queries";

import { useCreateBackfill } from "./useCreateBackfill";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock("openapi/queries", () => ({
  useBackfillServiceCreateBackfill: vi.fn(),
  useBackfillServiceListBackfillsUiKey: "useBackfillServiceListBackfillsUiKey",
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: { readonly children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: queryClient }, children);

const makeQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { gcTime: Infinity, retry: false, staleTime: Infinity },
    },
  });

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("useCreateBackfill — mutate body shape (mutual-exclusion gate)", () => {
  let mockMutate: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockMutate = vi.fn();
    vi.mocked(useBackfillServiceCreateBackfill).mockReturnValue({
      isPending: false,
      mutate: mockMutate,
    } as unknown as ReturnType<typeof useBackfillServiceCreateBackfill>);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("partitioned: sends partition_date_start/end and omits from_date/to_date entirely", async () => {
    const queryClient = makeQueryClient();

    const { result } = renderHook(
      () => useCreateBackfill({ isPartitioned: true, onSuccessConfirm: vi.fn() }),
      {
        wrapper: createWrapper(queryClient),
      },
    );

    await act(async () => {
      result.current.createBackfill({
        requestBody: {
          dag_id: "test_dag",
          dag_run_conf: null,
          from_date: "",
          max_active_runs: 1,
          partition_date_end: "2024-01-31T00:00",
          partition_date_start: "2024-01-01T00:00",
          reprocess_behavior: "none",
          run_backwards: false,
          run_on_latest_version: true,
          to_date: "",
        },
      });
    });

    expect(mockMutate).toHaveBeenCalledTimes(1);
    expect(mockMutate).toHaveBeenCalledWith({
      requestBody: {
        dag_id: "test_dag",
        dag_run_conf: null,
        max_active_runs: 1,
        partition_date_end: new Date("2024-01-31T00:00").toISOString(),
        partition_date_start: new Date("2024-01-01T00:00").toISOString(),
        reprocess_behavior: "none",
        run_backwards: false,
        run_on_latest_version: true,
      },
    });
  });

  it("non-partitioned: sends from_date/to_date and omits partition_date_start/end entirely", async () => {
    const queryClient = makeQueryClient();

    const { result } = renderHook(
      () => useCreateBackfill({ isPartitioned: false, onSuccessConfirm: vi.fn() }),
      { wrapper: createWrapper(queryClient) },
    );

    await act(async () => {
      result.current.createBackfill({
        requestBody: {
          dag_id: "test_dag",
          dag_run_conf: null,
          from_date: "2024-01-01T00:00",
          max_active_runs: 1,
          partition_date_end: "",
          partition_date_start: "",
          reprocess_behavior: "none",
          run_backwards: false,
          run_on_latest_version: true,
          to_date: "2024-01-31T00:00",
        },
      });
    });

    expect(mockMutate).toHaveBeenCalledTimes(1);
    expect(mockMutate).toHaveBeenCalledWith({
      requestBody: {
        dag_id: "test_dag",
        dag_run_conf: null,
        from_date: new Date("2024-01-01T00:00").toISOString(),
        max_active_runs: 1,
        reprocess_behavior: "none",
        run_backwards: false,
        run_on_latest_version: true,
        to_date: new Date("2024-01-31T00:00").toISOString(),
      },
    });
  });

  it("at-cap: partition_date_start === partition_date_end is allowed (pinning > not >=)", async () => {
    const queryClient = makeQueryClient();

    const { result } = renderHook(
      () => useCreateBackfill({ isPartitioned: true, onSuccessConfirm: vi.fn() }),
      {
        wrapper: createWrapper(queryClient),
      },
    );

    await act(async () => {
      result.current.createBackfill({
        requestBody: {
          dag_id: "test_dag",
          dag_run_conf: null,
          from_date: "",
          max_active_runs: 1,
          partition_date_end: "2024-01-15T00:00",
          partition_date_start: "2024-01-15T00:00",
          reprocess_behavior: "none",
          run_backwards: false,
          run_on_latest_version: true,
          to_date: "",
        },
      });
    });

    // start === end: must reach mutate (not blocked)
    expect(mockMutate).toHaveBeenCalledTimes(1);
    expect(mockMutate).toHaveBeenCalledWith({
      requestBody: {
        dag_id: "test_dag",
        dag_run_conf: null,
        max_active_runs: 1,
        partition_date_end: new Date("2024-01-15T00:00").toISOString(),
        partition_date_start: new Date("2024-01-15T00:00").toISOString(),
        reprocess_behavior: "none",
        run_backwards: false,
        run_on_latest_version: true,
      },
    });
  });

  it("over-cap: partition_date_start > partition_date_end is blocked — mutate is never called", async () => {
    const queryClient = makeQueryClient();

    const { result } = renderHook(
      () => useCreateBackfill({ isPartitioned: true, onSuccessConfirm: vi.fn() }),
      {
        wrapper: createWrapper(queryClient),
      },
    );

    await act(async () => {
      result.current.createBackfill({
        requestBody: {
          dag_id: "test_dag",
          dag_run_conf: null,
          from_date: "",
          max_active_runs: 1,
          partition_date_end: "2024-01-01T00:00",
          partition_date_start: "2024-01-31T00:00",
          reprocess_behavior: "none",
          run_backwards: false,
          run_on_latest_version: true,
          to_date: "",
        },
      });
    });

    // start > end: must NOT reach mutate
    expect(mockMutate).not.toHaveBeenCalled();
  });
});
