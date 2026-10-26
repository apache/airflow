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
import { createElement, type PropsWithChildren } from "react";

import { renderHook, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import { isExactTryView, useTaskInstanceView } from "./useTaskInstanceView";

vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => false,
}));

afterEach(() => vi.restoreAllMocks());

describe("isExactTryView", () => {
  it.each([
    ["try_number=2&region_id=r&region_index=1", true],
    ["try_number=2&region_id=r", false],
    ["try_number=2&region_index=1", false],
    ["region_id=r&region_index=1", false],
    ["", false],
  ])("treats %s as exact-try: %s", (search, expected) => {
    expect(isExactTryView(new URLSearchParams(search))).toBe(expected);
  });
});

const REGION_ID = "11111111-1111-4111-8111-111111111111";

const Wrapper = ({ children }: PropsWithChildren) =>
  createElement(
    BaseWrapper,
    undefined,
    createElement(
      MemoryRouter,
      { initialEntries: [`/?try_number=1&region_id=${REGION_ID}&region_index=0`] },
      children,
    ),
  );

describe("useTaskInstanceView", () => {
  it.each([
    { expected: true, liveId: "newer", liveTry: 2 },
    { expected: false, liveId: "exact", liveTry: 1 },
  ])(
    "is historical only once the live task instance has settled (live $liveId, expected $expected)",
    async ({ expected, liveId, liveTry }) => {
      const release = new AbortController();
      const liveTaskInstance = { id: liveId, try_number: liveTry } as TaskInstanceResponse;

      vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockReturnValue(
        new Promise<TaskInstanceResponse>((resolve) => {
          release.signal.addEventListener("abort", () => resolve(liveTaskInstance));
        }) as ReturnType<typeof TaskInstanceService.getMappedTaskInstance>,
      );
      const tryDetails = vi
        .spyOn(TaskInstanceService, "getTaskInstanceTryDetails")
        .mockResolvedValue({ id: "exact", try_number: 1 } as TaskInstanceHistoryResponse);

      const { result } = renderHook(() => useTaskInstanceView(), { wrapper: Wrapper });

      await waitFor(() => expect(tryDetails).toHaveBeenCalled());
      await waitFor(() => expect(result.current.isLoading).toBe(true));
      expect(result.current.historical).toBe(false);
      expect(result.current.taskInstance).toBeUndefined();

      release.abort();

      await waitFor(() => expect(result.current.isLoading).toBe(false));
      expect(result.current.historical).toBe(expected);
    },
  );
});
