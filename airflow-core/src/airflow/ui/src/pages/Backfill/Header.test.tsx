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
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as Queries from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

const mocks = vi.hoisted(() => ({
  cancelBackfill: vi.fn(),
  pauseBackfill: vi.fn(),
  unpauseBackfill: vi.fn(),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof Queries>();

  return {
    ...actual,
    useBackfillServiceCancelBackfill: () => ({ isPending: false, mutate: mocks.cancelBackfill }),
    useBackfillServicePauseBackfill: () => ({ isPending: false, mutate: mocks.pauseBackfill }),
    useBackfillServiceUnpauseBackfill: () => ({ isPending: false, mutate: mocks.unpauseBackfill }),
    useDagServiceGetDag: () => ({ data: undefined }),
  };
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) =>
      ({
        "common:backfill_one": "Backfill",
        "components:banner.cancel": "Cancel",
        "components:banner.pause": "Pause",
        "components:banner.unpause": "Unpause",
      })[key] ?? key,
  }),
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => undefined,
}));

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

describe("Backfill header", () => {
  beforeEach(() => {
    mocks.cancelBackfill.mockReset();
    mocks.pauseBackfill.mockReset();
    mocks.unpauseBackfill.mockReset();
  });

  it("renders metadata without loading every Dag run", () => {
    render(<Header backfill={backfill} />, { wrapper: Wrapper });

    expect(screen.getByText("Backfill #7")).toBeInTheDocument();
  });

  it("renders duration when the backfill is completed", () => {
    render(<Header backfill={{ ...backfill, completed_at: "2026-07-02T00:00:00Z" }} />, {
      wrapper: Wrapper,
    });

    expect(screen.queryByText("—")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel" })).not.toBeInTheDocument();
  });

  it("can pause and cancel an active backfill", () => {
    render(<Header backfill={backfill} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(mocks.pauseBackfill).toHaveBeenCalledWith({ backfillId: 7 });
    expect(mocks.cancelBackfill).toHaveBeenCalledWith({ backfillId: 7 });
  });

  it("can unpause a paused backfill", () => {
    render(<Header backfill={{ ...backfill, is_paused: true }} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: "Unpause" }));

    expect(mocks.unpauseBackfill).toHaveBeenCalledWith({ backfillId: 7 });
  });
});
