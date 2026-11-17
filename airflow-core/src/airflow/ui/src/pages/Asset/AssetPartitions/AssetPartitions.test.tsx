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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type * as ReactI18Next from "react-i18next";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AssetService } from "openapi/requests";
import type { AssetPartitionResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { AssetPartitions } from "./AssetPartitions";
import { groupByMonth } from "./partitionStatus";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => 25 }));

vi.mock("react-i18next", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactI18Next>()),
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

afterEach(() => vi.restoreAllMocks());

const partition = (
  key: string,
  status: AssetPartitionResponse["status"],
  extra: Partial<AssetPartitionResponse> = {},
): AssetPartitionResponse => ({
  consumer_run_count: 0,
  last_event_id: null,
  last_event_timestamp: null,
  last_producer_run: null,
  partition_key: key,
  status,
  ...extra,
});

const renderPartitions = (partitions: Array<AssetPartitionResponse>) => {
  const getAssetPartitions = vi.spyOn(AssetService, "getAssetPartitions").mockResolvedValue({
    partitions,
    status_counts: { failed: 1, in_progress: 0, materialized: 2 },
    total_entries: partitions.length,
  });

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/assets/1/partitions"]}>
        <TimezoneProvider>
          <Routes>
            <Route element={<AssetPartitions />} path="/assets/:assetId/partitions" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

  return getAssetPartitions;
};

describe("AssetPartitions", () => {
  it("shows the count of each status and one row per key", async () => {
    renderPartitions([
      partition("2026-01-02", "failed", {
        consumer_run_count: 7,
        last_producer_run: {
          dag_id: "producer",
          end_date: null,
          run_id: "scheduled__2026-01-02",
          start_date: null,
          state: "failed",
        },
      }),
      partition("2026-01-01", "materialized"),
    ]);

    expect(await screen.findByRole("link", { name: "producer" })).toHaveAttribute(
      "href",
      "/dags/producer/runs/scheduled__2026-01-02",
    );
    expect(screen.getByRole("cell", { name: "7" })).toBeInTheDocument();
    expect(screen.getByTestId("partition-status-materialized")).toHaveTextContent("2");
    expect(screen.getByTestId("partition-status-failed")).toHaveTextContent("1");
    expect(screen.getByTestId("partition-status-in_progress")).toHaveTextContent("0");
  });

  it("draws one day cell per key when every key is a day", async () => {
    renderPartitions([partition("2026-01-02", "failed"), partition("2026-01-01", "materialized")]);

    await waitFor(() => expect(screen.getAllByTestId("partition-day-cell")).toHaveLength(2));
    expect(screen.getAllByTestId("partition-day-cell").map((cell) => cell.dataset.status)).toEqual([
      "materialized",
      "failed",
    ]);
  });

  it("draws no day grid when a key is not a day", async () => {
    renderPartitions([partition("2026-01-01", "materialized"), partition("emea", "materialized")]);

    expect(await screen.findByText("emea")).toBeInTheDocument();
    expect(screen.queryByTestId("partition-day-grid")).not.toBeInTheDocument();
  });

  it("filters by the statuses that are switched on", async () => {
    const getAssetPartitions = renderPartitions([partition("2026-01-01", "materialized")]);

    fireEvent.click(await screen.findByTestId("partition-status-failed"));
    await waitFor(() =>
      expect(getAssetPartitions).toHaveBeenLastCalledWith(expect.objectContaining({ status: ["failed"] })),
    );
    expect(screen.getByTestId("partition-status-failed")).toHaveAttribute("aria-pressed", "true");

    fireEvent.click(screen.getByTestId("partition-status-failed"));
    await waitFor(() =>
      expect(screen.getByTestId("partition-status-failed")).toHaveAttribute("aria-pressed", "false"),
    );
  });
});

describe("groupByMonth", () => {
  it("orders months by date and keys days by day of month", () => {
    const months = groupByMonth([
      partition("2026-02-01T00:00:00", "failed"),
      partition("2026-01-31", "materialized"),
    ]);

    expect(months?.map(({ days, month }) => [month, [...days.keys()]])).toEqual([
      ["2026-01", [31]],
      ["2026-02", [1]],
    ]);
  });

  it.each([
    [[]],
    [[partition("2026-01-01T00", "materialized")]],
    [[partition("2026-01-32", "materialized")]],
    [[partition("2026-01-01T00:00:00", "failed"), partition("2026-01-01", "materialized")]],
  ])("returns nothing for %j", (partitions) => {
    expect(groupByMonth(partitions)).toBeUndefined();
  });
});
