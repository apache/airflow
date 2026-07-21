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
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { Backfills } from "./Backfills";

const mocks = vi.hoisted(() => ({
  listBackfills: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillsUi: mocks.listBackfills,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => (key === "common:completed" ? "Completed" : key),
  }),
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useParams: () => ({ dagId: "example_dag" }) };
});

vi.mock("src/components/Time", () => ({
  default: ({ datetime }: { readonly datetime: string | null }) => <span>{datetime}</span>,
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 25 : undefined),
}));

const makeBackfill = (overrides: Partial<BackfillResponse> = {}): BackfillResponse => ({
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
  ...overrides,
});

describe("Backfills", () => {
  beforeEach(() => mocks.listBackfills.mockReset());

  it("links each backfill to its Dag runs screen without loading progress", () => {
    const backfills = [makeBackfill(), makeBackfill({ completed_at: "2026-07-06T00:00:00Z", id: 8 })];

    mocks.listBackfills.mockReturnValue({
      data: { backfills, total_entries: backfills.length },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<Backfills />, { wrapper: Wrapper });

    const destinations = screen.getAllByRole("link").map((link) => link.getAttribute("href"));

    expect(destinations).toContain("/dags/example_dag/backfills/7");
    expect(destinations).toContain("/dags/example_dag/backfills/8");
    expect(screen.queryByText("table.progress")).not.toBeInTheDocument();
    expect(mocks.listBackfills).toHaveBeenCalledWith({
      dagId: "example_dag",
      limit: 25,
      offset: 0,
    });
  });
});
