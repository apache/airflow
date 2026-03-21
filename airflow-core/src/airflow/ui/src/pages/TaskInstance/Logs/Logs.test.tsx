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
  await waitFor(() => {
    const virtualizedList = screen.getByTestId("virtualized-list");
    const virtualizedItems = virtualizedList.querySelectorAll('[data-testid^="virtualized-item-"]');

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

    const summarySource = screen.getByTestId(
      'summary-Log message source details sources=["/home/airflow/logs/dag_id=tutorial_dag/run_id=manual__2025-02-28T05:18:54.249762+00:00/task_id=load/attempt=1.log"]',
    );

    expect(summarySource).toBeVisible();
    fireEvent.click(summarySource);
    await waitFor(() => expect(screen.queryByText(/sources=\[/iu)).toBeVisible());

    const summaryPre = screen.getByTestId("summary-Pre task execution logs");

    expect(summaryPre).toBeVisible();
    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.getByText(/starting attempt 1 of 3/iu)).toBeVisible());

    const summaryPost = screen.getByTestId("summary-Post task execution logs");

    expect(summaryPost).toBeVisible();
    fireEvent.click(summaryPost);
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).toBeVisible());

    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).not.toBeVisible());
    fireEvent.click(summaryPre);
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).toBeVisible());

    fireEvent.click(summaryPost);
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).not.toBeVisible());
    fireEvent.click(summaryPost);
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).toBeVisible());

    const settingsBtn = screen.getByRole("button", { name: /settings/iu });

    fireEvent.click(settingsBtn);

    const expandItem = await screen.findByRole("menuitem", { name: /expand/iu });

    fireEvent.click(expandItem);

    /* ─── NEW: open again & click  "Collapse"  ─── */
    fireEvent.click(settingsBtn); // menu is closed after previous click, so reopen
    const collapseItem = await screen.findByRole("menuitem", { name: /collapse/iu });

    fireEvent.click(collapseItem);

    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).not.toBeVisible());
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
});
