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
import type * as ReactI18Next from "react-i18next";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AssetService } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { AssetDeadlines } from "./AssetDeadlines";

vi.mock("react-i18next", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactI18Next>()),
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

afterEach(() => vi.restoreAllMocks());

describe("AssetDeadlines", () => {
  it("renders empty state when no deadlines exist", async () => {
    vi.spyOn(AssetService, "getAssetDeadlines").mockResolvedValue({
      alerts: [],
      deadlines: [],
    });

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/assets/1/deadlines"]}>
          <TimezoneProvider>
            <Routes>
              <Route element={<AssetDeadlines />} path="/assets/:assetId/deadlines" />
            </Routes>
          </TimezoneProvider>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(await screen.findByText("assets:deadlines.emptyState")).toBeInTheDocument();
  });

  it("renders producer contract and consumer deadlines with pending, late, and resolved instances", async () => {
    vi.spyOn(AssetService, "getAssetDeadlines").mockResolvedValue({
      alerts: [
        {
          created_at: "2026-11-13T00:00:00Z",
          cron: "0 9 * * *",
          dag_id: "orders_producer",
          id: "alert-1",
          interval: null,
          is_producer: true,
          name: "daily_contract",
        },
        {
          created_at: "2026-11-13T00:00:00Z",
          cron: null,
          dag_id: "orders_consumer",
          id: "alert-2",
          interval: { hours: 6 },
          is_producer: false,
          name: null,
        },
      ],
      deadlines: [
        {
          created_at: "2026-11-13T00:00:00Z",
          deadline_alert_id: "alert-1",
          deadline_time: "2026-11-14T09:00:00Z",
          id: "dl-1",
          is_late: false,
          missed: false,
          partition_key: "2026-11-14",
          resolved_at: null,
        },
        {
          created_at: "2026-11-12T00:00:00Z",
          deadline_alert_id: "alert-1",
          deadline_time: "2026-11-13T09:00:00Z",
          id: "dl-2",
          is_late: true,
          missed: true,
          partition_key: "2026-11-13",
          resolved_at: null,
        },
        {
          created_at: "2026-11-11T00:00:00Z",
          deadline_alert_id: "alert-1",
          deadline_time: "2026-11-12T09:00:00Z",
          id: "dl-3",
          is_late: false,
          missed: true,
          partition_key: "2026-11-12",
          resolved_at: "2026-11-12T09:20:00Z",
        },
      ],
    });

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/assets/1/deadlines"]}>
          <TimezoneProvider>
            <Routes>
              <Route element={<AssetDeadlines />} path="/assets/:assetId/deadlines" />
            </Routes>
          </TimezoneProvider>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(await screen.findByRole("link", { name: "orders_producer" })).toHaveAttribute(
      "href",
      "/dags/orders_producer",
    );
    expect(screen.getByText("assets:deadlines.contractBadge")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "orders_consumer" })).toHaveAttribute(
      "href",
      "/dags/orders_consumer",
    );
    expect(screen.getByText("assets:deadlines.consumerBadge")).toBeInTheDocument();
    expect(screen.getByText("assets:deadlines.pending")).toBeInTheDocument();
    expect(screen.getByText("assets:deadlines.late")).toBeInTheDocument();
    expect(screen.getByText("assets:deadlines.resolved")).toBeInTheDocument();
  });
});
