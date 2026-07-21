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

import type { BackfillDagRunResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillDagRuns } from "./BackfillDagRuns";

const mocks = vi.hoisted(() => ({
  useBackfillServiceListBackfillDagRuns: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillDagRuns: mocks.useBackfillServiceListBackfillDagRuns,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => (key === "states.success" ? "Success" : key),
  }),
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useParams: () => ({ backfillId: "7", dagId: "example_dag" }) };
});

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 25 : undefined),
}));

const dagRuns: Array<BackfillDagRunResponse> = [
  {
    backfill_id: 7,
    dag_id: "example_dag",
    dag_run_id: null,
    dag_run_state: null,
    exception_reason: "slot skipped",
    id: 1,
    logical_date: null,
    partition_key: "partition-a",
    sort_ordinal: 1,
  },
  {
    backfill_id: 7,
    dag_id: "example_dag",
    dag_run_id: "scheduled__2026-07-02",
    dag_run_state: "success",
    exception_reason: null,
    id: 2,
    logical_date: "2026-07-02T00:00:00Z",
    partition_key: null,
    sort_ordinal: 2,
  },
];

describe("BackfillDagRuns", () => {
  beforeEach(() => mocks.useBackfillServiceListBackfillDagRuns.mockReset());

  it("renders partition slots, skipped reasons, and links to created Dag runs", () => {
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: dagRuns, total_entries: dagRuns.length },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(screen.getByText("partition-a")).toBeInTheDocument();
    expect(screen.getByText("slot skipped")).toBeInTheDocument();
    expect(screen.getByText("Success")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "scheduled__2026-07-02" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/scheduled__2026-07-02",
    );
  });

  it("passes URL pagination to the API request", () => {
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: [], total_entries: 0 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(mocks.useBackfillServiceListBackfillDagRuns).toHaveBeenCalledWith({
      backfillId: 7,
      limit: 25,
      offset: 0,
    });
  });
});
