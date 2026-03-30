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

/**
 * Calendar data fixture — creates success + failed DAG runs for calendar tests.
 */

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
import dayjs from "dayjs";
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import {
  apiCreateDagRun,
  safeCleanupDagRun,
  apiSetDagRunState,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

export type CalendarRunsData = {
  dagId: string;
};

export const test = base.extend<Record<never, never>, { calendarRunsData: CalendarRunsData }>({
  calendarRunsData: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dagId = testConfig.testDag.id;
      const createdRunIds: Array<string> = [];

      await waitForDagReady(authenticatedRequest, dagId);

      const now = dayjs();
      const yesterday = now.subtract(1, "day");
      const baseDate = yesterday.isSame(now, "month") ? yesterday : now;

      const workerHourOffset = workerInfo.parallelIndex * 2;
      const successIso = baseDate
        .startOf("day")
        .hour(2 + workerHourOffset)
        .toISOString();
      const failedIso = baseDate
        .startOf("day")
        .hour(3 + workerHourOffset)
        .toISOString();

      const successRunId = uniqueRunId("cal_success");
      const failedRunId = uniqueRunId("cal_failed");

      try {
        await apiCreateDagRun(authenticatedRequest, dagId, {
          dag_run_id: successRunId,
          logical_date: successIso,
        });
        createdRunIds.push(successRunId);
        await apiSetDagRunState(authenticatedRequest, { dagId, runId: successRunId, state: "success" });

        await apiCreateDagRun(authenticatedRequest, dagId, {
          dag_run_id: failedRunId,
          logical_date: failedIso,
        });
        createdRunIds.push(failedRunId);
        await apiSetDagRunState(authenticatedRequest, { dagId, runId: failedRunId, state: "failed" });

        await use({ dagId });
      } finally {
        for (const runId of createdRunIds) {
          await safeCleanupDagRun(authenticatedRequest, dagId, runId);
        }
      }
    },
    { scope: "worker", timeout: 180_000 },
  ],
});
