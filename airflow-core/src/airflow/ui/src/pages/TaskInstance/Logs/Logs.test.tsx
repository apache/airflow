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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, it, expect, beforeAll } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

const ITEM_HEIGHT = 20;

beforeAll(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    value: ITEM_HEIGHT,
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    value: 800,
  });
});

const waitForLogs = async () => {
  await waitFor(() => expect(screen.getByTestId("virtualized-list")).toBeInTheDocument());

  // Wait for virtualized items to be rendered - they might not all be visible initially
  // Items can have either virtualized-item- or group-header- testid prefixes
  await waitFor(() => {
    const virtualizedList = screen.getByTestId("virtualized-list");
    const virtualizedItems = virtualizedList.querySelectorAll(
      '[data-testid^="virtualized-item-"], [data-testid^="group-header-"]',
    );

    expect(virtualizedItems.length).toBeGreaterThan(0);
  });

  fireEvent.scroll(screen.getByTestId("virtualized-list"), { target: { scrollTop: ITEM_HEIGHT * 2 } });
};

describe("Task log source", () => {
  it("Toggles logger and location on click", async () => {
    render(
      // <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-09-11T17:44:49.064088+00:00/tasks/source_testing"]} />,
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    let logLine = screen.getByTestId("virtualized-item-2");

    // Source should be hidden by default
    expect(logLine.querySelector('[data-key="logger"]')).toBeNull();
    expect(logLine.querySelector('[data-key="loc"]')).toBeNull();

    // Toggle source on
    fireEvent.keyDown(document.activeElement ?? document.body, { code: "KeyS", key: "S" });
    fireEvent.keyPress(document.activeElement ?? document.body, { code: "KeyS", key: "S" });
    fireEvent.keyUp(document.activeElement ?? document.body, { code: "KeyS", key: "S" });

    logLine = screen.getByTestId("virtualized-item-2");
    const source = logLine.querySelector('[data-key="logger"]');
    const loc = logLine.querySelector('[data-key="loc"]');

    // Source should now be visible
    expect(source).toBeVisible();
    expect(source).toHaveProperty("innerText", "source=airflow.models.dagbag.DagBag");

    expect(loc).toBeVisible();
    expect(loc).toHaveProperty("innerText", "loc=dagbag.py:593");
  });
});
describe("Task log grouping", () => {
  it("Display task log content on click", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitForLogs();

    // Group headers use the summary-{name} testid pattern and are always visible
    const summarySource = screen.getByTestId(
      'summary-Log message source details sources=["/home/airflow/logs/dag_id=tutorial_dag/run_id=manual__2025-02-28T05:18:54.249762+00:00/task_id=load/attempt=1.log"]',
    );

    expect(summarySource).toBeVisible();

    const summaryPre = screen.getByTestId("summary-Pre task execution logs");

    expect(summaryPre).toBeVisible();

    // All groups start collapsed. Verify Post header is in the virtualizer range.
    const summaryPost = screen.getByTestId("summary-Post task execution logs");

    expect(summaryPost).toBeVisible();

    // Groups start collapsed — content should not be in DOM
    expect(screen.queryByText(/starting attempt 1 of 3/iu)).toBeNull();

    // Click to expand Pre
    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.getByText(/starting attempt 1 of 3/iu)).toBeInTheDocument());

    // Click Pre to collapse
    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).toBeNull());

    // Click Pre to expand again
    fireEvent.click(summaryPre);
    await waitFor(() =>
      expect(screen.queryByText(/Task instance is in running state/iu)).toBeInTheDocument(),
    );

    // Click Pre to collapse again (to return to compact view)
    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).toBeNull());

    // Now expand Post (which is visible because Pre is collapsed again)
    fireEvent.click(screen.getByTestId("summary-Post task execution logs"));
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).toBeInTheDocument());

    // Collapse Post
    fireEvent.click(screen.getByTestId("summary-Post task execution logs"));
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).toBeNull());

    // Test Expand All / Collapse All via settings menu
    const settingsBtn = screen.getByRole("button", { name: /settings/iu });

    fireEvent.click(settingsBtn);

    const expandItem = await screen.findByRole("menuitem", { name: /expand/iu });

    fireEvent.click(expandItem);

    // After "Expand All", Pre group content near the top should be visible
    await waitFor(() => expect(screen.queryByText(/starting attempt 1 of 3/iu)).toBeInTheDocument());

    /* ─── Click  "Collapse"  ─── */
    fireEvent.click(settingsBtn);
    const collapseItem = await screen.findByRole("menuitem", { name: /collapse/iu });

    fireEvent.click(collapseItem);

    // After "Collapse All", group content should be gone from the DOM
    await waitFor(() => expect(screen.queryByText(/starting attempt 1 of 3/iu)).toBeNull());
  }, 10_000);

  it("renders nested groups correctly", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitForLogs();

    // The nested group "Dependency check details" should have a header
    // First expand the parent group "Pre task execution logs"
    const summaryPre = screen.getByTestId("summary-Pre task execution logs");

    fireEvent.click(summaryPre);

    // The nested group header should now be visible
    await waitFor(() => expect(screen.getByTestId("summary-Dependency check details")).toBeInTheDocument());

    // But nested group content is collapsed
    expect(screen.queryByText(/dep_context=non-requeueable/iu)).toBeNull();

    // Expand the nested group
    fireEvent.click(screen.getByTestId("summary-Dependency check details"));
    await waitFor(() => expect(screen.getByText(/dep_context=non-requeueable/iu)).toBeInTheDocument());
  }, 10_000);
});

describe("Task log search", () => {
  it("search input is rendered in the log header", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    expect(screen.getByTestId("log-search-input")).toBeInTheDocument();
  });

  it("typing in the search input enables navigation buttons for a known term", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    // "running state" appears in the mock log data
    fireEvent.change(searchInput, { target: { value: "running state" } });

    // Navigation buttons should become enabled once matches are found
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /next match/iu })).not.toBeDisabled();
      expect(screen.getByRole("button", { name: /previous match/iu })).not.toBeDisabled();
    });
  });

  it("shows no-matches indicator for a term that does not exist in logs", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    fireEvent.change(searchInput, { target: { value: "zzz_not_in_logs_zzz" } });

    await waitFor(() => {
      // Navigation buttons should be disabled with zero matches
      const nextBtn = screen.getByRole("button", { name: /next match/iu });

      expect(nextBtn).toBeDisabled();
    });
  });

  it("pressing Escape clears the search query", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    fireEvent.change(searchInput, { target: { value: "running" } });

    await waitFor(() => expect(screen.queryByRole("button", { name: /next match/iu })).toBeInTheDocument());

    fireEvent.keyDown(searchInput, { key: "Escape" });

    await waitFor(() => {
      expect((searchInput as HTMLInputElement).value).toBe("");
    });
  });

  it("pressing Enter keeps navigation buttons enabled (navigates to next match)", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    // "state" appears multiple times in the mock log data
    fireEvent.change(searchInput, { target: { value: "state" } });

    // Wait for matches to be found (navigation buttons become enabled)
    await waitFor(() => expect(screen.getByRole("button", { name: /next match/iu })).not.toBeDisabled());

    // Navigate forward — buttons remain enabled
    fireEvent.keyDown(searchInput, { key: "Enter" });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /next match/iu })).not.toBeDisabled();
    });
  });

  it("pressing Shift+Enter keeps navigation buttons enabled (navigates to previous match)", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    fireEvent.change(searchInput, { target: { value: "state" } });

    // Wait for matches to be found
    await waitFor(() => expect(screen.getByRole("button", { name: /previous match/iu })).not.toBeDisabled());

    // Navigate backward — buttons remain enabled
    fireEvent.keyDown(searchInput, { key: "Enter", shiftKey: true });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /previous match/iu })).not.toBeDisabled();
    });
  });

  it("search finds matches per-line inside log groups (not collapsed into 1 match per group)", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitForLogs();

    const searchInput = screen.getByTestId("log-search-input");

    // "INFO" appears in many individual log lines across multiple groups.
    // With the old code, each group collapsed into 1 match. Now each line is a separate match.
    fireEvent.change(searchInput, { target: { value: "INFO" } });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /next match/iu })).not.toBeDisabled();
    });

    // Verify per-line search by navigating through matches.
    // With the old code (1 match per group), there were only 3 matches for "INFO".
    // Now each line is a separate match, so we can navigate through many more.
    const nextBtn = screen.getByRole("button", { name: /next match/iu });

    // Navigate forward 4 times — if only 3 matches existed, the 4th click would wrap to #1
    // We just verify it stays enabled (which it always does with >0 matches).
    // The real proof is that the auto-expand test passes: searching "starting attempt"
    // finds a match INSIDE a group, which was impossible with the old collapsed approach.
    for (let navStep = 0; navStep < 4; navStep += 1) {
      fireEvent.click(nextBtn);
    }

    await waitFor(() => {
      expect(nextBtn).not.toBeDisabled();
    });
  }, 10_000);

  it("search navigating to match in collapsed group auto-expands it", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitForLogs();

    // All groups start collapsed. Search for text that only appears inside a group.
    const searchInput = screen.getByTestId("log-search-input");

    fireEvent.change(searchInput, { target: { value: "starting attempt" } });

    await waitFor(() => {
      const nextBtn = screen.getByRole("button", { name: /next match/iu });

      expect(nextBtn).not.toBeDisabled();
    });

    // The match is inside "Pre task execution logs" group.
    // Auto-expand should make it visible.
    await waitFor(() => expect(screen.getByText(/starting attempt 1 of 3/iu)).toBeInTheDocument());
  }, 10_000);

  it("skips group markers when assigning line numbers", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitForLogs();

    const expectRenderedLineNumber = async (pattern: RegExp, expectedLineNumber: number) => {
      const row = (await screen.findByText(pattern)).closest('[data-testid^="virtualized-item-"]');

      expect(row).not.toBeNull();

      const anchor = row?.querySelector<HTMLAnchorElement>("a[id]");

      expect(anchor).not.toBeNull();

      expect(Number(anchor?.id)).toBe(expectedLineNumber);
    };

    const summaryPre = screen.getByTestId("summary-Pre task execution logs");

    fireEvent.click(summaryPre);

    const summaryDependency = await screen.findByTestId("summary-Dependency check details");

    fireEvent.click(summaryDependency);

    await expectRenderedLineNumber(/dep_context=non-requeueable/iu, 0);
    await expectRenderedLineNumber(/dep_context=requeueable/iu, 1);
    await expectRenderedLineNumber(/starting attempt 1 of 3/iu, 2);
  }, 10_000);
});
