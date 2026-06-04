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
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";

import {
  useDagRunServiceGetDagRunsKey,
  useGridServiceGetGridRunsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";

import { useGridTiSummariesStream } from "./useGridTISummaries";

// Mock useAutoRefresh to avoid real timer scheduling
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return {
    ...actual,
    useAutoRefresh: vi.fn(() => false),
  };
});

const createMockResponse = (chunks: Array<string>) => {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      chunks.forEach((chunk) => {
        controller.enqueue(encoder.encode(chunk));
      });
      controller.close();
    },
  });

  return {
    body: stream,
    ok: true,
  } as unknown as Response;
};

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: { readonly children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

describe("useGridTiSummariesStream", () => {
  let mockFetch: Mock;

  beforeEach(() => {
    mockFetch = vi.fn().mockImplementation(() => Promise.resolve(createMockResponse([])));
    vi.stubGlobal("fetch", mockFetch);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("streams summaries correctly on mount", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          gcTime: Infinity,
          staleTime: Infinity,
        },
      },
    });
    const wrapper = createWrapper(queryClient);

    const chunk = `${JSON.stringify({ run_id: "run_1", state: "success", task_id: "task_1" })}\n`;

    mockFetch.mockResolvedValueOnce(createMockResponse([chunk]));

    const { result } = renderHook(() => useGridTiSummariesStream({ dagId: "dag_1", runIds: ["run_1"] }), {
      wrapper,
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(result.current.summariesByRunId.get("run_1")).toEqual({
      run_id: "run_1",
      state: "success",
      task_id: "task_1",
    });
  });

  it("buffers state updates and applies them once per chunk", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          gcTime: Infinity,
          staleTime: Infinity,
        },
      },
    });
    const wrapper = createWrapper(queryClient);

    const chunk = [
      JSON.stringify({ run_id: "run_1", state: "success" }),
      JSON.stringify({ run_id: "run_2", state: "failed" }),
      "",
    ].join("\n");

    mockFetch.mockResolvedValueOnce(createMockResponse([chunk]));

    const { result } = renderHook(
      () => useGridTiSummariesStream({ dagId: "dag_1", runIds: ["run_1", "run_2"] }),
      { wrapper },
    );

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(result.current.summariesByRunId.size).toBe(2);
    expect(result.current.summariesByRunId.get("run_1")).toEqual({ run_id: "run_1", state: "success" });
    expect(result.current.summariesByRunId.get("run_2")).toEqual({ run_id: "run_2", state: "failed" });
  });

  it("coalesces multiple cache invalidation events into a single refresh tick", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          gcTime: Infinity,
          staleTime: Infinity,
        },
      },
    });
    const wrapper = createWrapper(queryClient);

    // Prepopulate cache so invalidations have matching queries to act on
    queryClient.setQueryData([useTaskInstanceServiceGetTaskInstancesKey], {});
    queryClient.setQueryData([useGridServiceGetGridRunsKey], {});
    queryClient.setQueryData([useDagRunServiceGetDagRunsKey], {});

    mockFetch.mockImplementation(() => Promise.resolve(createMockResponse([])));

    renderHook(() => useGridTiSummariesStream({ dagId: "dag_1", runIds: ["run_1"] }), { wrapper });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    mockFetch.mockClear();

    // Trigger multiple invalidations synchronously
    act(() => {
      void queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] });
      void queryClient.invalidateQueries({ queryKey: [useGridServiceGetGridRunsKey] });
      void queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] });
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    // Should only trigger fetch ONCE despite 3 watched keys being invalidated
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("aborts active stream when a new refresh is scheduled", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          gcTime: Infinity,
          staleTime: Infinity,
        },
      },
    });
    const wrapper = createWrapper(queryClient);

    // Prepopulate cache so invalidation has a matching query to act on
    queryClient.setQueryData([useTaskInstanceServiceGetTaskInstancesKey], {});

    let resolveReaderPromise: (value: ReadableStreamReadResult<Uint8Array>) => void;
    const readerPromise = new Promise<ReadableStreamReadResult<Uint8Array>>((resolve) => {
      resolveReaderPromise = resolve;
    });

    const mockReader = {
      cancel: vi.fn(() => Promise.resolve()),
      read: vi.fn(() => readerPromise),
    };

    const stream = {
      getReader: () => mockReader,
    };

    const mockResponse = {
      body: stream,
      ok: true,
    } as unknown as Response;

    mockFetch.mockResolvedValueOnce(mockResponse);

    const { result } = renderHook(() => useGridTiSummariesStream({ dagId: "dag_1", runIds: ["run_1"] }), {
      wrapper,
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);

    // Trigger invalidation to force re-fetch
    mockFetch.mockImplementationOnce(() => Promise.resolve(createMockResponse([])));
    act(() => {
      void queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] });
    });

    await act(async () => {
      await vi.runAllTimersAsync();
    });

    expect(mockReader.cancel).toHaveBeenCalled();
    expect(mockFetch).toHaveBeenCalledTimes(2);

    // Resolve the first stream reader's read with a chunk to verify it is ignored
    await act(async () => {
      const valueBytes = new TextEncoder().encode(
        `${JSON.stringify({ run_id: "run_1", state: "success" })}\n`,
      );

      resolveReaderPromise({
        done: false,
        value: valueBytes,
      });
      await vi.runAllTimersAsync();
    });

    // The state should NOT be updated with the aborted stream's value
    expect(result.current.summariesByRunId.get("run_1")).toBeUndefined();
  });
});
