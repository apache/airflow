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

import { act, renderHook, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import type { FilterConfig, FilterValue } from "src/components/FilterBar/types";

import { BaseWrapper } from "src/utils/Wrapper";

import { useRememberedFilters } from "./useRememberedFilters";

const CONFIGS: Array<FilterConfig> = [
  { key: "state", label: "State", type: "select" },
  { key: "tags", label: "Tags", matchModeKey: "tags_match_mode", type: "multiselect" },
  {
    endKey: "start_date_lte",
    key: "start_date",
    label: "Start",
    startKey: "start_date_gte",
    type: "daterange",
  },
  {
    fromSearchParams: () => undefined,
    key: "run_state",
    label: "Run state",
    toSearchParams: () => ({ dag_run_state: undefined, last_dag_run_state: undefined }),
    type: "select",
  },
];

const createWrapper =
  (initialEntries: Array<string>) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

const renderRemembered = (initialEntries: Array<string>) =>
  renderHook(
    () => {
      const [, setSearchParams] = useSearchParams();

      const onFiltersChange = useRememberedFilters(CONFIGS, (filters: Record<string, FilterValue>) => {
        const params = new URLSearchParams();

        Object.entries(filters).forEach(([key, value]) => {
          if (typeof value === "string" || typeof value === "number") {
            params.set(key, String(value));
          }
        });

        setSearchParams(params, { replace: true });
      });

      return { navigate: useNavigate(), onFiltersChange, search: useLocation().search };
    },
    { wrapper: createWrapper(initialEntries) },
  );

beforeEach(() => {
  localStorage.clear();
});

describe("useRememberedFilters", () => {
  it("restores the remembered filters on a bare landing", async () => {
    localStorage.setItem("dags-last-filters", JSON.stringify("state=running"));

    const { result } = renderRemembered(["/dags"]);

    await waitFor(() => expect(result.current.search).toBe("?state=running"));
  });

  it("remembers only the params the filter bar owns", async () => {
    renderRemembered([
      "/dags?state=running&tags=a&tags=b&tags_match_mode=all&start_date_gte=2024&last_dag_run_state=failed&name_pattern=foo&offset=40",
    ]);

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-last-filters") ?? '""')).toBe(
        "state=running&tags=a&tags=b&tags_match_mode=all&start_date_gte=2024&last_dag_run_state=failed",
      );
    });
  });

  it("leaves an explicit query alone", () => {
    localStorage.setItem("dags-last-filters", JSON.stringify("state=running"));

    const { result } = renderRemembered(["/dags?state=queued"]);

    expect(result.current.search).toBe("?state=queued");
  });

  it("keeps a pinned default preset ahead of the remembered filters", () => {
    localStorage.setItem("dags-preset-filters-default", JSON.stringify("Pinned"));
    localStorage.setItem("dags-last-filters", JSON.stringify("state=running"));

    const { result } = renderRemembered(["/dags"]);

    expect(result.current.search).toBe("");
    // Neither the restore nor the landing may erase what is remembered.
    expect(localStorage.getItem("dags-last-filters")).toBe(JSON.stringify("state=running"));
  });

  it("forgets the filters when they are cleared through the filter bar", async () => {
    const { result } = renderRemembered(["/dags?state=running"]);

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-last-filters") ?? '""')).toBe("state=running");
    });

    act(() => result.current.onFiltersChange({}));

    await waitFor(() => {
      expect(result.current.search).toBe("");
      expect(JSON.parse(localStorage.getItem("dags-last-filters") ?? '""')).toBe("");
    });
  });

  it("restores the filters when the link for the current page is followed", async () => {
    const { result } = renderRemembered(["/dags?state=running"]);

    await waitFor(() => {
      expect(JSON.parse(localStorage.getItem("dags-last-filters") ?? '""')).toBe("state=running");
    });

    // The sidebar link to the page you are already on drops the query rather than the filters.
    act(() => {
      void result.current.navigate("/dags");
    });

    await waitFor(() => expect(result.current.search).toBe("?state=running"));
  });
});
