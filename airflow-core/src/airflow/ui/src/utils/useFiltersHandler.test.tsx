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

import { act, renderHook } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { isValidFilterValue } from "src/components/FilterBar/utils";

import { SearchParamsKeys } from "src/constants/searchParams";
import { BaseWrapper } from "src/utils/Wrapper";

import { useFiltersHandler } from "./useFiltersHandler";

const createWrapper =
  (initialEntries: Array<string> = ["/dags"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

const renderFiltersHandler = (initialEntries?: Array<string>) =>
  renderHook(() => useFiltersHandler([SearchParamsKeys.TAGS]), {
    wrapper: createWrapper(initialEntries),
  });

const renderBooleanHandler = (initialEntries?: Array<string>) =>
  renderHook(() => useFiltersHandler([SearchParamsKeys.NEEDS_REVIEW]), {
    wrapper: createWrapper(initialEntries),
  });

afterEach(() => {
  localStorage.clear();
});

describe("useFiltersHandler multiselect reads", () => {
  it("reads a repeated param into an array", () => {
    const { result } = renderFiltersHandler(["/dags?tags=production&tags=ml"]);

    expect(result.current.initialValues[SearchParamsKeys.TAGS]).toEqual(["production", "ml"]);
  });

  it("omits the key when the param is absent", () => {
    const { result } = renderFiltersHandler();

    expect(result.current.initialValues[SearchParamsKeys.TAGS]).toBeUndefined();
  });

  it("ignores empty values", () => {
    const { result } = renderFiltersHandler(["/dags?tags="]);

    expect(result.current.initialValues[SearchParamsKeys.TAGS]).toBeUndefined();
  });
});

describe("useFiltersHandler multiselect writes", () => {
  it("appends one entry per value rather than joining them", () => {
    const { result } = renderFiltersHandler();

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.TAGS]: ["production", "ml"] }));

    expect(result.current.searchParams.getAll(SearchParamsKeys.TAGS)).toEqual(["production", "ml"]);
  });

  it("replaces every previous value", () => {
    const { result } = renderFiltersHandler(["/dags?tags=production&tags=ml"]);

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.TAGS]: ["ops"] }));

    expect(result.current.searchParams.getAll(SearchParamsKeys.TAGS)).toEqual(["ops"]);
  });

  it("clears the match mode once the last value is removed", () => {
    const { result } = renderFiltersHandler(["/dags?tags=production&tags=ml&tags_match_mode=all"]);

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.TAGS]: [] }));

    expect(result.current.searchParams.getAll(SearchParamsKeys.TAGS)).toEqual([]);
    expect(result.current.searchParams.get(SearchParamsKeys.TAGS_MATCH_MODE)).toBeNull();
  });

  it("keeps the match mode while values remain", () => {
    const { result } = renderFiltersHandler(["/dags?tags=production&tags=ml&tags_match_mode=all"]);

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.TAGS]: ["production"] }));

    expect(result.current.searchParams.get(SearchParamsKeys.TAGS_MATCH_MODE)).toBe("all");
  });

  it("drops pagination params on every write", () => {
    const { result } = renderFiltersHandler(["/dags?offset=30&cursor=abc"]);

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.TAGS]: ["production"] }));

    expect(result.current.searchParams.get(SearchParamsKeys.OFFSET)).toBeNull();
    expect(result.current.searchParams.get(SearchParamsKeys.CURSOR)).toBeNull();
  });
});

describe("useFiltersHandler boolean filters", () => {
  it("reads an enabled boolean from the URL", () => {
    const { result } = renderBooleanHandler(["/dags?needs_review=true"]);

    expect(result.current.initialValues[SearchParamsKeys.NEEDS_REVIEW]).toBe("true");
  });

  // A boolean filter is on only while its param says so. "false" is not a second state to
  // filter by — it means the same as no filter, so it must not resurrect the pill.
  it("treats an explicit false as unset", () => {
    const { result } = renderBooleanHandler(["/dags?needs_review=false"]);

    expect(isValidFilterValue("boolean", result.current.initialValues[SearchParamsKeys.NEEDS_REVIEW])).toBe(
      false,
    );
  });

  it("writes only the enabled state back to the URL", () => {
    const { result } = renderBooleanHandler();

    act(() => result.current.handleFiltersChange({ [SearchParamsKeys.NEEDS_REVIEW]: "true" }));
    expect(result.current.searchParams.get(SearchParamsKeys.NEEDS_REVIEW)).toBe("true");

    act(() => result.current.handleFiltersChange({}));
    expect(result.current.searchParams.get(SearchParamsKeys.NEEDS_REVIEW)).toBeNull();
  });
});
