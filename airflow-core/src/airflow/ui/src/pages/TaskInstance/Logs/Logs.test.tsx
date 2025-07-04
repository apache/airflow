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
import { setupServer, type SetupServerApi } from "msw/node";
import { afterEach, describe, it, expect, beforeAll, afterAll } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServerApi;
const ITEM_HEIGHT = 20;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    value: ITEM_HEIGHT,
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    value: 800,
  });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Task log grouping", () => {
  it("Display task log content on click", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );

    await waitFor(() => expect(screen.getByTestId("virtualized-list")).toBeInTheDocument());
    await waitFor(() => expect(screen.getByTestId("virtualized-item-0")).toBeInTheDocument());

    fireEvent.scroll(screen.getByTestId("virtualized-list"), { target: { scrollTop: ITEM_HEIGHT * 2 } });

    await waitFor(() => expect(screen.getByTestId("virtualized-item-2")).toBeInTheDocument());

    const summarySource = screen.getByTestId(
      'summary-Log message source details: sources=["/home/airflow/logs/dag_id=tutorial_dag/run_id=manual__2025-02-28T05:18:54.249762+00:00/task_id=load/attempt=1.log"]',
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

    const expandBtn = screen.getByRole("button", { name: /expand\.expand/iu });

    fireEvent.click(expandBtn);

    expect(screen.getByText(/Marking task as SUCCESS/iu)).toBeVisible();

    const collapseBtn = screen.getByRole("button", { name: /expand\.collapse/iu });

    fireEvent.click(collapseBtn);
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).not.toBeVisible());
    await waitFor(() => expect(screen.queryByText(/Marking task as SUCCESS/iu)).not.toBeVisible());
  }, 10_000);
});
