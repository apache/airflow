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
import { act, renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { dagsFilterKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { BaseWrapper } from "src/utils/Wrapper";

import { useDagsFilterParams } from "./useDagsFilterParams";

const createWrapper =
  (initialEntries: Array<string> = ["/"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

const saveFilter = (key: SearchParamsKeys, value: string) => {
  localStorage.setItem(dagsFilterKey(key), JSON.stringify(value));
};

afterEach(() => {
  localStorage.clear();
});

describe("useDagsFilterParams", () => {
  it("falls back to saved DAG list filters when URL params are absent", () => {
    saveFilter(SearchParamsKeys.PAUSED, "true");
    saveFilter(SearchParamsKeys.FAVORITE, "false");
    saveFilter(SearchParamsKeys.LAST_DAG_RUN_STATE, "failed");
    saveFilter(SearchParamsKeys.NEEDS_REVIEW, "true");

    const { result } = renderHook(() => useDagsFilterParams(), {
      wrapper: createWrapper(),
    });

    expect(result.current.pausedFilter).toBe("true");
    expect(result.current.favoriteFilter).toBe("false");
    expect(result.current.lastDagRunStateFilter).toBe("failed");
    expect(result.current.needsReviewFilter).toBe("true");
  });

  it("prefers URL filters over saved values", () => {
    saveFilter(SearchParamsKeys.PAUSED, "true");
    saveFilter(SearchParamsKeys.FAVORITE, "false");
    saveFilter(SearchParamsKeys.LAST_DAG_RUN_STATE, "failed");
    saveFilter(SearchParamsKeys.NEEDS_REVIEW, "true");

    const { result } = renderHook(() => useDagsFilterParams(), {
      wrapper: createWrapper(["/?paused=false&favorite=true&last_dag_run_state=success&needs_review=false"]),
    });

    expect(result.current.pausedFilter).toBe("false");
    expect(result.current.favoriteFilter).toBe("true");
    expect(result.current.lastDagRunStateFilter).toBe("success");
    expect(result.current.needsReviewFilter).toBe("false");
  });

  it("persists changed filters", () => {
    const { result } = renderHook(() => useDagsFilterParams(), {
      wrapper: createWrapper(),
    });

    act(() => {
      result.current.setPausedFilter("false");
      result.current.setFavoriteFilter("true");
      result.current.setLastDagRunStateFilter("running");
      result.current.setNeedsReviewFilter("true");
    });

    expect(JSON.parse(localStorage.getItem(dagsFilterKey(SearchParamsKeys.PAUSED)) ?? "null")).toBe(
      "false",
    );
    expect(JSON.parse(localStorage.getItem(dagsFilterKey(SearchParamsKeys.FAVORITE)) ?? "null")).toBe(
      "true",
    );
    expect(
      JSON.parse(localStorage.getItem(dagsFilterKey(SearchParamsKeys.LAST_DAG_RUN_STATE)) ?? "null"),
    ).toBe("running");
    expect(
      JSON.parse(localStorage.getItem(dagsFilterKey(SearchParamsKeys.NEEDS_REVIEW)) ?? "null"),
    ).toBe("true");
  });
});
