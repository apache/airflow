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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { Stats } from "./Stats";

vi.mock("openapi/queries", () => ({
  useDashboardServiceDagStats: () => ({
    data: {
      active_dag_count: 1,
      failed_dag_count: 0,
      queued_dag_count: 0,
      running_dag_count: 0,
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

describe("Dashboard stats", () => {
  it("links the active Dag count to the exact active scheduling state", () => {
    render(<Stats />, { wrapper: Wrapper });

    expect(screen.getByText("stats.activeDags").closest("a")).toHaveAttribute(
      "href",
      "/dags?scheduling_state=active",
    );
  });
});
