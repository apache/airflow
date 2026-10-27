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
import { expect } from "@playwright/test";

import { test } from "tests/e2e/fixtures/calendar-data";

test.describe("Dag Calendar Tab", () => {
  test.setTimeout(90_000);

  // calendarRunsData is triggered once per worker via beforeEach.
  test.beforeEach(async ({ calendarRunsData, dagCalendarTab }) => {
    test.setTimeout(60_000);
    await dagCalendarTab.navigateToCalendar(calendarRunsData.dagId);
  });

  test("verify success and failed manual runs render as active cells", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    expect(await dagCalendarTab.getActiveCellCount()).toBeGreaterThan(0);

    const states = await dagCalendarTab.getManualRunStates();

    expect.soft(states).toContain("success");
    expect.soft(states).toContain("failed");
  });
});
