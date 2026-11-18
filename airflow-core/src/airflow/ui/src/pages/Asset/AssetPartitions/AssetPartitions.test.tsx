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
import type { AssetPartitionResponse, AssetPartitionStatusCounts } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { AssetPartitions } from "./AssetPartitions";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => 25 }));

vi.mock("react-i18next", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactI18Next>()),
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  localStorage.clear();
});

const partition = (
  key: string,
  status: AssetPartitionResponse["status"],
  extra: Partial<AssetPartitionResponse> = {},
): AssetPartitionResponse => ({
  consumer_runs: [],
  last_event_id: null,
  last_event_timestamp: null,
  last_producer_run: null,
  partition_key: key,
  status,
  ...extra,
});

const renderPartitions = (
  partitions: Array<AssetPartitionResponse>,
  statusCounts: AssetPartitionStatusCounts = { failed: 1, in_progress: 0, materialized: 2 },
  initialEntry = "/assets/1/partitions",
) => {
  const getAssetPartitions = vi.spyOn(AssetService, "getAssetPartitions").mockResolvedValue({
    partitions,
    status_counts: statusCounts,
    total_entries: partitions.length,
  });

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
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
        consumer_runs: [
          {
            dag_id: "consumer",
            data_interval_end: null,
            data_interval_start: null,
            end_date: null,
            logical_date: null,
            partition_key: "2026-01-02",
            run_id: "consumed__2026-01-02",
            start_date: null,
            state: "success",
            triggering: true,
          },
        ],
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
    expect(screen.getByRole("link", { name: "consumer" })).toHaveAttribute(
      "href",
      "/dags/consumer/runs/consumed__2026-01-02",
    );
    expect(screen.getByTestId("partition-status-materialized")).toHaveTextContent("2");
    expect(screen.getByTestId("partition-status-failed")).toHaveTextContent("1");
    expect(screen.getByTestId("partition-status-in_progress")).toHaveTextContent("0");
  });

  it("filters by the statuses in the URL and does not filter from the counts", async () => {
    const getAssetPartitions = renderPartitions(
      [partition("2026-01-01", "failed")],
      undefined,
      "/assets/1/partitions?partition_status=failed&partition_status=in_progress",
    );

    await waitFor(() =>
      expect(getAssetPartitions).toHaveBeenLastCalledWith(
        expect.objectContaining({ status: ["failed", "in_progress"] }),
      ),
    );
    expect(screen.getByText("failed, in_progress")).toBeInTheDocument();

    const requests = getAssetPartitions.mock.calls.length;

    fireEvent.click(screen.getByTestId("partition-status-materialized"));
    expect(getAssetPartitions).toHaveBeenCalledTimes(requests);
  });

  it("sorts by the clicked column", async () => {
    const getAssetPartitions = renderPartitions([partition("2026-01-01", "materialized")]);

    expect(await screen.findByText("2026-01-01")).toBeInTheDocument();
    expect(getAssetPartitions).toHaveBeenLastCalledWith(
      expect.objectContaining({ orderBy: "-partition_key" }),
    );

    fireEvent.click(screen.getByText("assets:lastAssetEvent"));
    await waitFor(() =>
      expect(getAssetPartitions).toHaveBeenLastCalledWith(
        expect.objectContaining({ orderBy: "last_event_timestamp" }),
      ),
    );
  });

  it("refreshes only while a partition is in progress", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const getAssetPartitions = renderPartitions([], { failed: 0, in_progress: 1, materialized: 0 });

    await waitFor(() => expect(getAssetPartitions).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(26_000);
    await waitFor(() => expect(getAssetPartitions.mock.calls.length).toBeGreaterThan(1));
  });

  it("does not refresh when no partition is in progress", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const getAssetPartitions = renderPartitions([], { failed: 1, in_progress: 0, materialized: 0 });

    await waitFor(() => expect(getAssetPartitions).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(60_000);
    expect(getAssetPartitions).toHaveBeenCalledTimes(1);
  });
});
