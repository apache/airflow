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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { NextRunAssetEventResponse, NextRunAssetsResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { AssetSchedule } from "./AssetSchedule";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string, options?: { count?: number; total?: number }) =>
      options?.count === undefined ? key : `${key}:${options.count} of ${options.total}`,
  }),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useAssetServiceGetDagAssetQueuedEvents: vi.fn(),
    useAssetServiceNextRunAssets: vi.fn(),
  };
});

const { useAssetServiceGetDagAssetQueuedEvents, useAssetServiceNextRunAssets } =
  await import("openapi/queries");

const makeEvent = (id: number, name: string): NextRunAssetEventResponse => ({
  asset_inactive: false,
  id,
  is_rollup: false,
  last_update: null,
  mapper_error: false,
  name,
  received_count: 0,
  received_keys: [],
  required_count: 1,
  required_keys: [],
  uri: `s3://bucket/${name}`,
});

const nextRunResponse = (nextRun: NextRunAssetsResponse) =>
  ({ data: nextRun, error: null, isFetching: false, isLoading: false }) as ReturnType<
    typeof useAssetServiceNextRunAssets
  >;

const queuedEventsResponse = () =>
  ({
    data: { queued_events: [], total_entries: 0 },
    error: null,
    isFetching: false,
    isLoading: false,
  }) as ReturnType<typeof useAssetServiceGetDagAssetQueuedEvents>;

const renderSchedule = (nextRun: NextRunAssetsResponse) => {
  vi.mocked(useAssetServiceNextRunAssets).mockReturnValue(nextRunResponse(nextRun));
  vi.mocked(useAssetServiceGetDagAssetQueuedEvents).mockReturnValue(queuedEventsResponse());

  render(
    <AssetSchedule dagId="dag_id" timetablePartitioned={false} timetableSummary="Every day at midnight" />,
    { wrapper: Wrapper },
  );
};

describe("AssetSchedule", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows the Dag's full asset total when the caller may read only some of them", () => {
    renderSchedule({
      events: [makeEvent(1, "visible_asset")],
      scheduling_asset_count: 3,
    });

    expect(screen.getByRole("button")).toHaveTextContent("assetSchedule:0 of 3");
  });

  it("still renders an asset schedule when the caller may read none of the assets", () => {
    renderSchedule({ events: [], scheduling_asset_count: 3 });

    expect(screen.getByRole("button")).toHaveTextContent("assetSchedule:0 of 3");
    expect(screen.queryByText("Every day at midnight")).not.toBeInTheDocument();
  });

  it("falls back to the timetable summary when the Dag has no scheduling assets", () => {
    renderSchedule({ events: [], scheduling_asset_count: 0 });

    expect(screen.getByText("Every day at midnight")).toBeInTheDocument();
  });

  it("renders the single-asset view only when the Dag is scheduled on one asset", () => {
    renderSchedule({
      events: [makeEvent(1, "only_asset")],
      scheduling_asset_count: 1,
    });

    expect(screen.getByRole("link", { name: "only_asset" })).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
