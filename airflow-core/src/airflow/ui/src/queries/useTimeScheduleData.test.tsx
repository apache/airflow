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
import { afterEach, describe, expect, it, vi } from "vitest";

import { useTimeScheduleData } from "./useTimeScheduleData";

vi.mock("src/utils", () => ({ useAutoRefresh: () => 100 }));
const createResponse = (count: number) =>
  new Response(
    `${JSON.stringify({
      dag_run_count: count,
      items: [{ dag_id: "example", dag_run_id: "run-1", run_count: count }],
    })}\n`,
  );

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("useTimeScheduleData", () => {
  it("reads split UTF-8 batches and a final line without a newline", async () => {
    const bytes = new TextEncoder().encode(
      `${JSON.stringify({ dag_run_count: 1, items: [{ dag_id: "한글" }] })}\n\n${JSON.stringify({ dag_run_count: 2, items: [{ dag_id: "last" }] })}`,
    );
    const split = bytes.indexOf(0xed) + 1;
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(bytes.slice(0, split));
          controller.enqueue(bytes.slice(split));
          controller.close();
        },
      }),
    );

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));
    const queryClient = new QueryClient();
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result, unmount } = renderHook(() => useTimeScheduleData("time_scale=60"), { wrapper });

    await waitFor(() => expect(result.current.dagRunCount).toBe(3));
    expect(result.current.timelineItems.map((item) => item.dag_id)).toEqual(["한글", "last"]);
    await waitFor(() => expect(response.body?.locked).toBe(false));
    unmount();
    queryClient.clear();
  });

  it("cancels a blocked stream and releases its reader on unmount", async () => {
    const cancel = vi.fn();
    const response = new Response(new ReadableStream({ cancel }));

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));
    const queryClient = new QueryClient();
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { unmount } = renderHook(() => useTimeScheduleData("time_scale=60"), { wrapper });

    await waitFor(() => expect(response.body?.locked).toBe(true));
    unmount();
    await waitFor(() => expect(response.body?.locked).toBe(false));
    expect(cancel).toHaveBeenCalledOnce();
    queryClient.clear();
  });

  it("reports invalid JSON and releases the reader", async () => {
    const response = new Response("invalid\n");

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));
    const queryClient = new QueryClient();
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result, unmount } = renderHook(() => useTimeScheduleData("time_scale=60"), { wrapper });

    await waitFor(() => expect(result.current.error).toBeInstanceOf(SyntaxError));
    expect(response.body?.locked).toBe(false);
    expect(result.current.timelineItems).toEqual([]);
    unmount();
    queryClient.clear();
  });

  it("replaces the streamed snapshot on auto-refresh instead of appending duplicate bars", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const fetchStream = vi
      .fn()
      .mockResolvedValueOnce(createResponse(1))
      .mockImplementation(() => Promise.resolve(createResponse(2)));

    vi.stubGlobal("fetch", fetchStream);
    const queryClient = new QueryClient();
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result, unmount } = renderHook(() => useTimeScheduleData("time_scale=60"), { wrapper });

    await waitFor(() => expect(result.current.dagRunCount).toBe(1));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    await waitFor(() => expect(result.current.dagRunCount).toBe(2));
    expect(result.current.timelineItems).toHaveLength(1);
    expect(fetchStream.mock.calls.length).toBeGreaterThan(1);
    unmount();
    queryClient.clear();
  });
});
