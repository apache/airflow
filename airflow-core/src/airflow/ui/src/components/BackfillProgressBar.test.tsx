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
import { afterEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillProgressBar } from "./BackfillProgressBar";

const { mockCounts, mockOptions } = vi.hoisted(() => ({
  mockCounts: { current: undefined as Record<string, number> | undefined },
  mockOptions: [] as Array<{ refetchInterval: number | false }>,
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillDagRuns: (
    { state }: { state: Array<string> },
    _queryKey: unknown,
    options: { refetchInterval: number | false },
  ) => {
    mockOptions.push(options);
    if (mockCounts.current === undefined) {
      return { data: undefined };
    }
    const counts = mockCounts.current;

    return {
      data: { backfill_dag_runs: [], total_entries: state.reduce((sum, st) => sum + (counts[st] ?? 0), 0) },
    };
  },
  useDagRunServiceGetDagRuns: () => ({ data: undefined }),
  useDagServiceGetDagDetails: () => ({ data: undefined }),
}));

vi.mock("src/queries/useConfig", () => ({ useConfig: () => undefined }));

vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => 5000,
}));

afterEach(() => {
  mockCounts.current = undefined;
  mockOptions.length = 0;
});

describe("BackfillProgressBar", () => {
  it("shows finished over created runs, ignoring skipped slots", () => {
    mockCounts.current = { failed: 1, queued: 2, running: 1, success: 3 };

    render(<BackfillProgressBar backfillId={1} isRunning />, { wrapper: Wrapper });

    expect(screen.getByText("4/7")).toBeInTheDocument();
    expect(
      screen.getByText(
        i18n.t("backfill.progressSummary", { failed: 1, ns: "components", remaining: 3, success: 3 }),
      ),
    ).toBeInTheDocument();
  });

  it.each([
    { expected: 5000, isRunning: true },
    { expected: false, isRunning: false },
  ])("polls only while the backfill is running (isRunning=$isRunning)", ({ expected, isRunning }) => {
    mockCounts.current = { success: 1 };

    render(<BackfillProgressBar backfillId={1} isRunning={isRunning} />, { wrapper: Wrapper });

    expect(screen.getByText("1/1")).toBeInTheDocument();
    expect(new Set(mockOptions.map((options) => options.refetchInterval))).toEqual(new Set([expected]));
  });

  it("renders nothing while the counts are loading", () => {
    const { container } = render(<BackfillProgressBar backfillId={1} isRunning />, { wrapper: Wrapper });

    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the backfill has created no runs", () => {
    mockCounts.current = {};

    const { container } = render(<BackfillProgressBar backfillId={1} isRunning />, { wrapper: Wrapper });

    expect(container).toBeEmptyDOMElement();
  });
});
