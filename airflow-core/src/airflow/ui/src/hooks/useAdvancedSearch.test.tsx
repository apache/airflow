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
import { MemoryRouter, useSearchParams } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { advancedSearchKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { BaseWrapper } from "src/utils/Wrapper";

import { useAdvancedSearch } from "./useAdvancedSearch";

const createWrapper =
  (initialEntries: Array<string> = ["/dags"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

const renderAdvancedSearch = (key: string, initialEntries?: Array<string>) =>
  renderHook(
    () => {
      const [searchParams] = useSearchParams();

      return { advanced: useAdvancedSearch(key), searchParams };
    },
    { wrapper: createWrapper(initialEntries) },
  );

afterEach(() => {
  localStorage.clear();
});

describe("useAdvancedSearch reads", () => {
  it("is disabled when neither the URL nor localStorage has a value", () => {
    const { result } = renderAdvancedSearch("dags");

    expect(result.current.advanced.enabled).toBe(false);
  });

  it("is enabled when the key is listed in advanced_search", () => {
    const { result } = renderAdvancedSearch("dags", ["/dags?advanced_search=dags"]);

    expect(result.current.advanced.enabled).toBe(true);
  });

  it("keeps searchbars independent via per-key values", () => {
    const entries = ["/events?advanced_search=dag_id&advanced_search=run_id"];

    expect(renderAdvancedSearch("dag_id", entries).result.current.advanced.enabled).toBe(true);
    expect(renderAdvancedSearch("run_id", entries).result.current.advanced.enabled).toBe(true);
    expect(renderAdvancedSearch("task_id", entries).result.current.advanced.enabled).toBe(false);
  });

  it("falls back to the stored preference when the param is absent", () => {
    localStorage.setItem(advancedSearchKey("dags"), JSON.stringify(true));

    const { result } = renderAdvancedSearch("dags");

    expect(result.current.advanced.enabled).toBe(true);
  });

  it("honors an explicit off marker over a stored-on preference", () => {
    localStorage.setItem(advancedSearchKey("dags"), JSON.stringify(true));

    const { result } = renderAdvancedSearch("dags", ["/dags?advanced_search=-dags"]);

    expect(result.current.advanced.enabled).toBe(false);
  });
});

describe("useAdvancedSearch toggle", () => {
  it("adds the key to advanced_search when enabled", () => {
    const { result } = renderAdvancedSearch("dags");

    act(() => result.current.advanced.onToggle(true));

    expect(result.current.searchParams.getAll(SearchParamsKeys.ADVANCED_SEARCH)).toEqual(["dags"]);
    expect(result.current.advanced.enabled).toBe(true);
  });

  it("records an explicit off marker and keeps the other searchbars when disabled", () => {
    const { result } = renderAdvancedSearch("dag_id", [
      "/events?advanced_search=dag_id&advanced_search=run_id",
    ]);

    act(() => result.current.advanced.onToggle(false));

    expect(result.current.searchParams.getAll(SearchParamsKeys.ADVANCED_SEARCH)).toEqual([
      "run_id",
      "-dag_id",
    ]);
    expect(result.current.advanced.enabled).toBe(false);
  });

  it("writes an explicit off marker when disabled from no prior entry", () => {
    const { result } = renderAdvancedSearch("dags");

    act(() => result.current.advanced.onToggle(false));

    expect(result.current.searchParams.getAll(SearchParamsKeys.ADVANCED_SEARCH)).toEqual(["-dags"]);
    expect(result.current.advanced.enabled).toBe(false);
  });

  it("flips from off to on without leaving the off marker behind", () => {
    const { result } = renderAdvancedSearch("dags", ["/dags?advanced_search=-dags"]);

    act(() => result.current.advanced.onToggle(true));

    expect(result.current.searchParams.getAll(SearchParamsKeys.ADVANCED_SEARCH)).toEqual(["dags"]);
    expect(result.current.advanced.enabled).toBe(true);
  });

  it("does not duplicate the key when enabled while already present", () => {
    const { result } = renderAdvancedSearch("dags", ["/dags?advanced_search=dags"]);

    act(() => result.current.advanced.onToggle(true));

    expect(result.current.searchParams.getAll(SearchParamsKeys.ADVANCED_SEARCH)).toEqual(["dags"]);
  });

  it("persists the toggle to localStorage as well", () => {
    const { result } = renderAdvancedSearch("dags");

    act(() => result.current.advanced.onToggle(true));

    expect(localStorage.getItem(advancedSearchKey("dags"))).toBe(JSON.stringify(true));
  });
});
