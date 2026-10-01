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

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { Stats } from "./Stats";

vi.mock("openapi/queries", () => ({
  useDashboardServiceDagStats: () => ({
    data: {
      active_dag_count: 1,
      failed_dag_count: 2,
      queued_dag_count: 3,
      running_dag_count: 4,
    },
    isLoading: false,
  }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("src/components/NeedsReviewButton", () => ({ NeedsReviewButtonWithModal: () => null }));
vi.mock("src/utils", () => ({ useAutoRefresh: () => false }));
vi.mock("./DagImportErrors", () => ({ DagImportErrors: () => null }));
vi.mock("./PluginImportErrors", () => ({ PluginImportErrors: () => null }));

// Dashboard renders under the /home route. Mount Stats as that route's element so relative links
// would resolve against /home (producing /home/dags -> 404) unless the links are absolute.
const wrapperAtHome = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/home"]}>
      <Routes>
        <Route element={children} path="home" />
      </Routes>
    </MemoryRouter>
  </BaseWrapper>
);

describe("Dashboard stats", () => {
  it.each([
    { href: "/dags?last_dag_run_state=failed", label: "stats.failedDags" },
    { href: "/dags?dag_run_state=queued", label: "stats.queuedDags" },
    { href: "/dags?dag_run_state=running", label: "stats.runningDags" },
    { href: "/dags?scheduling_state=active", label: "stats.activeDags" },
  ])("links $label to the absolute $href from the /home route", ({ href, label }) => {
    render(<Stats />, { wrapper: wrapperAtHome });

    expect(screen.getByText(label).closest("a")).toHaveAttribute("href", href);
  });
});
