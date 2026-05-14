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

test.describe("DAG Calendar Tab", () => {
  test.setTimeout(90_000);

  // calendarRunsData is triggered once per worker via beforeEach.
  test.beforeEach(async ({ calendarRunsData, dagCalendarTab }) => {
    test.setTimeout(60_000);
    await dagCalendarTab.navigateToCalendar(calendarRunsData.dagId);
  });

  test("verify calendar grid renders", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();
  });

  test("verify active cells appear for DAG runs", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const count = await dagCalendarTab.getActiveCellCount();

    expect(count).toBeGreaterThan(0);
  });

  test("verify manual runs are detected", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const states = await dagCalendarTab.getManualRunStates();

    expect(states.length).toBeGreaterThanOrEqual(2);
  });

  test("verify hover shows correct run states", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const states = await dagCalendarTab.getManualRunStates();

    expect(states).toContain("success");
    expect(states).toContain("failed");
  });

  test("failed filter shows only failed runs", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const totalStates = await dagCalendarTab.getManualRunStates();

    expect(totalStates).toContain("success");
    expect(totalStates).toContain("failed");

    await dagCalendarTab.switchToFailedView();

    const failedStates = await dagCalendarTab.getManualRunStates();

    expect(failedStates).toContain("failed");
    expect(failedStates).not.toContain("success");
  });

  test("failed view reduces active cells", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const totalCount = await dagCalendarTab.getActiveCellCount();

    await dagCalendarTab.switchToFailedView();

    const failedCount = await dagCalendarTab.getActiveCellCount();

    expect(failedCount).toBeLessThan(totalCount);
  });

  test("color scale changes between total and failed view", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();

    const totalColors = await dagCalendarTab.getActiveCellColors();

    await dagCalendarTab.switchToFailedView();

    const failedColors = await dagCalendarTab.getActiveCellColors();

    expect(failedColors).not.toEqual(totalColors);
  });

  test("cells reflect failed view mode attribute", async ({ dagCalendarTab }) => {
    await dagCalendarTab.switchToHourly();
    await dagCalendarTab.switchToFailedView();

    const cell = dagCalendarTab.activeCells.first();

    await expect(cell).toHaveAttribute("data-view-mode", "failed");
  });
});
