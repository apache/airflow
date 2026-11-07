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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { usePersistentSearchParamKeys } from "src/constants/persistentSearchParams";
import { BaseWrapper } from "src/utils/Wrapper";

import { NavTabs } from "./NavTabs";

const tabs = [
  { label: "Details", value: "details" },
  { label: "Logs", search: "try_number=2", value: "logs" },
];

const DetailsTabs = () => <NavTabs persistentKeys={usePersistentSearchParamKeys()} tabs={tabs} />;

const renderTabs = (search: string, carryFilters = true) => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/my_dag/runs/my_run${search}`]}>
        {carryFilters ? <DetailsTabs /> : <NavTabs tabs={tabs} />}
      </MemoryRouter>
    </BaseWrapper>,
  );
};

const linkSearch = (name: string) => {
  const href = screen.getByRole("link", { name }).getAttribute("href") ?? "";

  return new URL(href, "http://localhost").searchParams;
};

describe("NavTabs", () => {
  it("carries the grid filters onto every tab", () => {
    renderTabs("?state=failed&run_type=manual&run_after_gte=2026-01-01");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      run_after_gte: "2026-01-01",
      run_type: "manual",
      state: "failed",
    });
  });

  it("carries the gantt date ranges by the start and end keys they write", () => {
    renderTabs("?start_date_gte=2026-01-01&end_date_lte=2026-02-01&start_date_range=ignored");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      end_date_lte: "2026-02-01",
      start_date_gte: "2026-01-01",
    });
  });

  it("carries the graph and task stream filters, mode included", () => {
    renderTabs(
      "?graph-operator=BashOperator&root=extract&upstream=true&downstream=true&depth=2&mode=traverse",
    );

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      depth: "2",
      downstream: "true",
      "graph-operator": "BashOperator",
      mode: "traverse",
      root: "extract",
      upstream: "true",
    });
  });

  it("carries a filter that repeats, rather than only its first value", () => {
    renderTabs("?log_level=error&log_level=warning");

    expect(linkSearch("Details").getAll("log_level")).toStrictEqual(["error", "warning"]);
  });

  it("leaves one tab's paging and sorting behind", () => {
    renderTabs("?state=failed&offset=20&limit=50&sort=-run_after");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({ state: "failed" });
  });

  it("lets the tab decide the parameters it names itself", () => {
    renderTabs("?state=failed&try_number=7");

    expect(Object.fromEntries(linkSearch("Logs"))).toStrictEqual({ state: "failed", try_number: "2" });
  });

  it("carries nothing unless the layout passes the keys", () => {
    renderTabs("?state=failed&log_level=error", false);

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({});
    expect(Object.fromEntries(linkSearch("Logs"))).toStrictEqual({ try_number: "2" });
  });
});
