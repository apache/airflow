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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../../public/i18n/locales/en/dag.json";
import { DeadlineStatus } from "./DeadlineStatus";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useDeadlinesServiceGetDeadlines: vi.fn() };
});

const { useDeadlinesServiceGetDeadlines } = await import("openapi/queries");

const buildAlert = (
  id: string,
  filter: DeadlineAlertResponse["run_type_filter"],
  interval: number,
): DeadlineAlertResponse => ({
  created_at: "2025-01-01T00:00:00Z",
  id,
  interval,
  reference_type: "DagRunQueuedAtDeadline",
  run_type_filter: filter,
});

const getCompletionRule = (interval: string) =>
  i18n.t("dag:deadlineAlerts.completionRule", {
    interval,
    reference: i18n.t("dag:deadlineAlerts.referenceType.DagRunQueuedAtDeadline"),
  });

describe("DeadlineStatus", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
  });

  it.each([
    { runTypeFilter: ["manual"], unrestrictedFilter: null },
    { runTypeFilter: ["manual"], unrestrictedFilter: undefined },
    { runTypeFilter: ["backfill", "manual"], unrestrictedFilter: null },
    { runTypeFilter: ["backfill", "manual"], unrestrictedFilter: undefined },
  ] satisfies Array<{
    runTypeFilter: DeadlineAlertResponse["run_type_filter"];
    unrestrictedFilter: null | undefined;
  }>)(
    "shows only applicable alert rules for $runTypeFilter with unrestricted filter $unrestrictedFilter",
    async ({ runTypeFilter, unrestrictedFilter }) => {
      vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
        data: {
          deadline_alerts: [
            buildAlert("manual", runTypeFilter, 30),
            buildAlert("scheduled", ["scheduled"], 3600),
            buildAlert("unrestricted", unrestrictedFilter, 120),
          ],
          deadlines: [],
          total_entries: 0,
        },
        isLoading: false,
      } as ReturnType<typeof useDeadlinesServiceGetDeadlines>);
      vi.useFakeTimers();

      try {
        render(<DeadlineStatus dagId="test_dag" dagRunId="manual_run" endDate={null} runType="manual" />, {
          wrapper: Wrapper,
        });

        await act(async () => {
          fireEvent.pointerEnter(screen.getByText(i18n.t("dag:deadlineStatus.met")));
          await vi.advanceTimersByTimeAsync(1500);
        });

        const tooltip = screen.getByRole("tooltip");

        expect(tooltip).toHaveTextContent(getCompletionRule("30 seconds"));
        expect(tooltip).toHaveTextContent(getCompletionRule("2 minutes"));
        expect(tooltip).not.toHaveTextContent(getCompletionRule("1 hour"));
      } finally {
        vi.useRealTimers();
      }
    },
  );
});
