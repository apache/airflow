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
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useDagRunServiceGetDagRun, useGridServiceGetLoopSummary } from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { useIsLoopGroup } from "./useIsLoopGroup";
import { useLoopSummary } from "./useLoopSummary";

const refetch = vi.fn();

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: vi.fn(),
  useGridServiceGetLoopSummary: vi.fn(),
}));
vi.mock("./useIsLoopGroup", () => ({ useIsLoopGroup: vi.fn() }));
vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  useAutoRefresh: () => 3000,
}));

const mockRunState = (state: string) =>
  vi
    .mocked(useDagRunServiceGetDagRun)
    .mockReturnValue({ data: { state } } as ReturnType<typeof useDagRunServiceGetDagRun>);

describe("useLoopSummary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useIsLoopGroup).mockReturnValue(true);
    vi.mocked(useGridServiceGetLoopSummary).mockReturnValue({
      data: { status: "failed" },
      refetch,
    } as unknown as ReturnType<typeof useGridServiceGetLoopSummary>);
  });

  it.each([
    ["running", 3000],
    ["success", false],
  ] as const)("uses DagRun %s liveness even when the loop has a failed body task", (state, interval) => {
    mockRunState(state);
    renderHook(() => useLoopSummary({ dagId: "dag", groupId: "loop", runId: "run" }), { wrapper: Wrapper });
    expect(useGridServiceGetLoopSummary).toHaveBeenLastCalledWith(
      expect.objectContaining({ dagId: "dag", groupId: "loop", runId: "run" }),
      undefined,
      expect.objectContaining({ enabled: true, refetchInterval: interval, retry: false }),
    );
  });

  it("does not request the summary of a group that is not a loop", () => {
    vi.mocked(useIsLoopGroup).mockReturnValue(false);
    mockRunState("running");
    renderHook(() => useLoopSummary({ dagId: "dag", groupId: "plain", runId: "run" }), { wrapper: Wrapper });
    expect(useGridServiceGetLoopSummary).toHaveBeenLastCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });

  it("refetches once when the run goes from pending to terminal", () => {
    mockRunState("running");
    const { rerender } = renderHook(() => useLoopSummary({ dagId: "dag", groupId: "loop", runId: "run" }), {
      wrapper: Wrapper,
    });

    expect(refetch).not.toHaveBeenCalled();
    mockRunState("success");
    rerender();
    expect(refetch).toHaveBeenCalledTimes(1);
    rerender();
    expect(refetch).toHaveBeenCalledTimes(1);
  });
});
