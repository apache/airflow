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
import { renderHook, waitFor } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { useResolvePassCoordinate } from "./useResolvePassCoordinate";

let mockParams: Record<string, string | undefined> = {};
let mockSearchParams = new URLSearchParams();
const setSearchParams = vi.fn();

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [mockSearchParams, setSearchParams] as const,
  };
});

vi.mock("openapi/queries", () => ({
  useTaskInstanceServiceGetTaskInstances: vi.fn(),
}));

const lookup = (row?: { region_id: string; region_index: number }, isFetching = false) =>
  ({ data: { task_instances: row === undefined ? [] : [row] }, isFetching }) as ReturnType<
    typeof useTaskInstanceServiceGetTaskInstances
  >;

describe("useResolvePassCoordinate", () => {
  beforeEach(() => {
    setSearchParams.mockClear();
    mockParams = { dagId: "d", runId: "r", taskId: "batch.work" };
    mockSearchParams = new URLSearchParams();
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(lookup());
  });

  it("does nothing when the url already names a row", () => {
    mockSearchParams = new URLSearchParams("region_id=own&region_index=3");

    const { result } = renderHook(() => useResolvePassCoordinate(), { wrapper: Wrapper });

    expect(result.current).toBe(false);
    const [, , options] = vi.mocked(useTaskInstanceServiceGetTaskInstances).mock.calls[0] ?? [];

    expect(options).toMatchObject({ enabled: false });
    expect(setSearchParams).not.toHaveBeenCalled();
  });

  it("asks for the row of that pass, narrowed to the mapped slot", () => {
    mockParams = { ...mockParams, mapIndex: "1" };
    mockSearchParams = new URLSearchParams("loop_id=batch&iteration=2");

    renderHook(() => useResolvePassCoordinate(), { wrapper: Wrapper });

    const [request] = vi.mocked(useTaskInstanceServiceGetTaskInstances).mock.calls[0] ?? [];

    expect(request).toMatchObject({ iteration: 2, loopId: "batch", mapIndex: [1], taskId: "batch.work" });
  });

  it("writes back the row's own coordinate, which the pass does not name", async () => {
    mockParams = { ...mockParams, mapIndex: "1" };
    mockSearchParams = new URLSearchParams("loop_id=batch&iteration=0");
    // A mapped expansion lives in its own region, so neither value follows from the pass.
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      lookup({ region_id: "expansion", region_index: 1 }),
    );

    renderHook(() => useResolvePassCoordinate(), { wrapper: Wrapper });

    await waitFor(() => expect(setSearchParams).toHaveBeenCalled());
    const [written, options] = setSearchParams.mock.calls[0] as [URLSearchParams, { replace: boolean }];

    expect(written.get("region_id")).toBe("expansion");
    expect(written.get("region_index")).toBe("1");
    // The same place, named properly, so it should not add a history entry.
    expect(options).toEqual({ replace: true });
  });

  it("holds while the row is unknown, so a looped task is never asked for unscoped", () => {
    mockSearchParams = new URLSearchParams("loop_id=batch&iteration=0");
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(lookup(undefined, true));

    const { result } = renderHook(() => useResolvePassCoordinate(), { wrapper: Wrapper });

    expect(result.current).toBe(true);
  });
});
