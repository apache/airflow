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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillResponse } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import BackfillBanner from "./BackfillBanner";

const mocks = vi.hoisted(() => ({
  cancelBackfill: vi.fn(),
  listBackfills: vi.fn(),
  pauseBackfill: vi.fn(),
  unpauseBackfill: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceCancelBackfill: () => ({ isPending: false, mutate: mocks.cancelBackfill }),
  useBackfillServiceListBackfillsUi: mocks.listBackfills,
  useBackfillServiceListBackfillsUiKey: "BackfillServiceListBackfillsUi",
  useBackfillServicePauseBackfill: () => ({ isPending: false, mutate: mocks.pauseBackfill }),
  useBackfillServiceUnpauseBackfill: () => ({ isPending: false, mutate: mocks.unpauseBackfill }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => (key === "banner.backfillInProgress" ? "Backfill in progress" : key),
  }),
}));

vi.mock("src/components/Time", () => ({
  default: ({ datetime }: { readonly datetime: string }) => <span>{datetime}</span>,
}));

vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof Utils>();

  return { ...actual, useAutoRefresh: () => 5000 };
});

const backfill: BackfillResponse = {
  completed_at: null,
  created_at: "2026-07-01T00:00:00Z",
  dag_display_name: "Example Dag",
  dag_id: "example_dag",
  dag_run_conf: null,
  from_date: "2026-07-01T00:00:00Z",
  id: 7,
  is_paused: false,
  max_active_runs: 4,
  reprocess_behavior: "failed",
  to_date: "2026-07-05T00:00:00Z",
  updated_at: "2026-07-01T00:00:00Z",
};

describe("BackfillBanner", () => {
  beforeEach(() => mocks.listBackfills.mockReset());

  it("links the active banner to its Dag runs screen", () => {
    mocks.listBackfills.mockReturnValue({
      data: { backfills: [backfill], total_entries: 1 },
      isLoading: false,
    });

    render(<BackfillBanner dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "Backfill in progress" })).toHaveAttribute(
      "href",
      "/dags/example_dag/backfills/7",
    );
  });
});
