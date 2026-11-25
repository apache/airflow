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
import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type * as Services from "openapi/requests/services.gen";

import i18n from "src/i18n/config";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillProgressBar } from "./BackfillProgressBar";

const { listBackfillDagRuns, mockCounts } = vi.hoisted(() => {
  const counts = { current: {} as Record<string, number> };

  return {
    listBackfillDagRuns: vi.fn(({ state }: { state: Array<string> }) =>
      Promise.resolve({
        backfill_dag_runs: [],
        total_entries: state.reduce((sum, st) => sum + (counts.current[st] ?? 0), 0),
      }),
    ),
    mockCounts: counts,
  };
});

vi.mock("openapi/requests/services.gen", async (importOriginal) => ({
  ...(await importOriginal<typeof Services>()),
  BackfillService: { listBackfillDagRuns },
}));

vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => 20,
}));

afterEach(() => {
  mockCounts.current = {};
  listBackfillDagRuns.mockClear();
});

describe("BackfillProgressBar", () => {
  it("shows finished over created runs, ignoring skipped slots", async () => {
    mockCounts.current = { failed: 1, none: 2, queued: 2, running: 1, success: 3 };

    render(<BackfillProgressBar backfillId={1} />, { wrapper: Wrapper });

    expect(await screen.findByText("4/7")).toBeInTheDocument();
    expect(
      screen.getByText(
        i18n.t("backfill.progressSummary", { failed: 1, ns: "components", remaining: 3, success: 3 }),
      ),
    ).toBeInTheDocument();
  });

  it("renders nothing when the backfill has created no runs", async () => {
    const { container } = render(<BackfillProgressBar backfillId={1} />, { wrapper: Wrapper });

    await waitFor(() => expect(listBackfillDagRuns).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it("refreshes the counts while shown", async () => {
    mockCounts.current = { running: 1 };

    render(<BackfillProgressBar backfillId={1} />, { wrapper: Wrapper });
    await screen.findByText("0/1");

    mockCounts.current = { success: 1 };

    expect(await screen.findByText("1/1")).toBeInTheDocument();
  });
});
