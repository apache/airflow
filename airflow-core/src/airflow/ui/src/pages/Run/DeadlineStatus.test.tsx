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

import type { DeadlineCollectionResponse, RunDeadlineAlertResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../../public/i18n/locales/en/dag.json";
import { DeadlineStatus } from "./DeadlineStatus";

const buildAlert = (id: string, isApplicable: boolean, interval: number): RunDeadlineAlertResponse => ({
  created_at: "2025-01-01T00:00:00Z",
  id,
  interval,
  is_applicable: isApplicable,
  reference_type: "DagRunQueuedAtDeadline",
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

  it("shows only alert rules marked applicable by the API", async () => {
    const deadlineData: DeadlineCollectionResponse = {
      deadline_alerts: [
        buildAlert("manual", true, 30),
        buildAlert("scheduled", false, 3600),
        buildAlert("unrestricted", true, 120),
      ],
      deadlines: [],
      total_entries: 0,
    };

    vi.useFakeTimers();

    try {
      render(
        <DeadlineStatus dagId="test_dag" dagRunId="manual_run" deadlineData={deadlineData} endDate={null} />,
        { wrapper: Wrapper },
      );

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
  });
});
