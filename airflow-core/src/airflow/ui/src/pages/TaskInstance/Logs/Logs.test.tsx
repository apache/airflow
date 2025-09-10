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

describe("Task log grouping", () => {
  it("Display task log content on click", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitFor(() => expect(screen.getByTestId("virtualized-list")).toBeInTheDocument());

    // Wait for virtualized items to be rendered - they might not all be visible initially
    await waitFor(() => {
      const virtualizedList = screen.getByTestId("virtualized-list");
      const virtualizedItems = virtualizedList.querySelectorAll('[data-testid^="virtualized-item-"]');

      expect(virtualizedItems.length).toBeGreaterThan(0);
    });

    fireEvent.scroll(screen.getByTestId("virtualized-list"), { target: { scrollTop: ITEM_HEIGHT * 2 } });

    // Wait for virtualized-item-2 to be rendered after scrolling
    await waitFor(
      () => {
        const virtualizedItem2 = screen.queryByTestId("virtualized-item-2");

        if (virtualizedItem2) {
          expect(virtualizedItem2).toBeInTheDocument();
        }
      },
      { timeout: 5000 },
    );

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

    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).toBeVisible());
  }, 10_000);
});
