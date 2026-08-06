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
import type { PropsWithChildren } from "react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { useDagsFilterModel } from "./useDagsFilterModel";

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => false,
}));

vi.mock("src/queries/useDagTagsInfinite", () => ({
  useDagTagsInfinite: () => ({
    data: { pages: [{ tags: ["example"] }] },
    error: null,
    fetchNextPage: vi.fn(),
    fetchPreviousPage: vi.fn(),
    hasNextPage: false,
    isFetching: false,
    refetch: vi.fn(),
  }),
}));

vi.mock("src/queries/useDagTimetableTypesInfinite", () => ({
  useDagTimetableTypesInfinite: () => ({
    data: { pages: [{ timetable_types: ["CronTriggerTimetable"] }] },
    error: null,
    fetchNextPage: vi.fn(),
    fetchPreviousPage: vi.fn(),
    hasNextPage: false,
    isFetching: false,
    refetch: vi.fn(),
  }),
}));

const createWrapper =
  (initialEntries: Array<string>) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

afterEach(() => localStorage.clear());

describe("useDagsFilterModel", () => {
  it("resets pagination, preserves unrelated params, and restores the prior URL on Back", async () => {
    const { result } = renderHook(
      () => ({ location: useLocation(), model: useDagsFilterModel(), navigate: useNavigate() }),
      { wrapper: createWrapper(["/dags?offset=20&future_filter=kept"]) },
    );

    act(() => result.current.model.timetableTypes.onChange(["CronTriggerTimetable"]));

    expect(result.current.location.search).toBe("?future_filter=kept&timetable_type=CronTriggerTimetable");

    act(() => void result.current.navigate(-1));

    await waitFor(() => expect(result.current.location.search).toBe("?offset=20&future_filter=kept"));
    expect(result.current.model.timetableTypes.values).toEqual([]);
  });

  it("clears known filters without dropping unrelated params", () => {
    const { result } = renderHook(() => ({ location: useLocation(), model: useDagsFilterModel() }), {
      wrapper: createWrapper([
        "/dags?paused=true&favorite=true&owners=airflow&tags=example&timetable_type=CronTriggerTimetable&future_filter=kept",
      ]),
    });

    act(() => result.current.model.clearAll());

    expect(result.current.location.search).toBe("?future_filter=kept");
  });
});
