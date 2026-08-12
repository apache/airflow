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
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { DeadlineAlertsBadge } from "./DeadlineAlertsBadge";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { interval?: string; reference?: string }) => {
      if (options?.reference === undefined) {
        return key;
      }

      return options.interval === undefined
        ? `${key}:${options.reference}`
        : `${key}:${options.interval}:${options.reference}`;
    },
  }),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDeadlinesServiceGetDagDeadlineAlerts: vi.fn(),
  };
});

const { useDeadlinesServiceGetDagDeadlineAlerts } = await import("openapi/queries");

// Defaults to a VariableInterval alert, whose interval only the scheduler resolves at evaluation
// time. Without a rule of its own, dayjs humanizes that null interval as "a few seconds" and the
// popover claims the run must complete within a few seconds of its logical date.
const baseAlert: DeadlineAlertResponse = {
  created_at: "2025-01-01T00:00:00Z",
  id: "alert-1",
  interval: null,
  name: null,
  reference_type: "DagRunLogicalDateDeadline",
};

const REFERENCE = "deadlineAlerts.referenceType.DagRunLogicalDateDeadline";
const DYNAMIC_RULE = `deadlineAlerts.completionRuleDynamic:${REFERENCE}`;
const FIXED_RULE = `deadlineAlerts.completionRule:an hour:${REFERENCE}`;

describe("DeadlineAlertsBadge", () => {
  it.each([
    { absent: FIXED_RULE, expected: DYNAMIC_RULE, interval: null },
    { absent: DYNAMIC_RULE, expected: FIXED_RULE, interval: 3600 },
  ])(
    "states the completion rule for an alert with interval $interval",
    async ({ absent, expected, interval }) => {
      vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
        data: { deadline_alerts: [{ ...baseAlert, interval }], total_entries: 1 },
      } as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);

      render(<DeadlineAlertsBadge dagId="test_dag" />, { wrapper: Wrapper });

      fireEvent.click(screen.getByRole("button"));

      expect(await screen.findByText(expected)).toBeInTheDocument();
      expect(screen.queryByText(absent)).not.toBeInTheDocument();
    },
  );
});
