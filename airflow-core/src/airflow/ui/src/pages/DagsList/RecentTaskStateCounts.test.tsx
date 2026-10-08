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
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { DAGRecentTaskInstanceStateCountsResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import "../../i18n/config";
import { RecentTaskStateCounts } from "./RecentTaskStateCounts";

const renderCounts = (
  entry: DAGRecentTaskInstanceStateCountsResponse | undefined,
  options: { isLoading?: boolean } = {},
) =>
  render(<RecentTaskStateCounts dagId="my_dag" entry={entry} isLoading={options.isLoading ?? false} />, {
    wrapper: ({ children }) => (
      <BaseWrapper>
        <MemoryRouter>{children}</MemoryRouter>
      </BaseWrapper>
    ),
  });

const makeEntry = (
  stateCounts: Record<string, number>,
  runIds: Array<string> = ["run_1"],
): DAGRecentTaskInstanceStateCountsResponse => ({
  dag_id: "my_dag",
  run_ids: runIds,
  state_counts: stateCounts,
});

const taskListLink = (runIdPattern: string, taskState: string) =>
  `/task_instances?dag_id_pattern=my_dag&run_id_pattern=${runIdPattern}&task_state=${taskState}`;

describe("RecentTaskStateCounts", () => {
  it("renders skeleton placeholders while loading", () => {
    renderCounts(undefined, { isLoading: true });
    expect(screen.getByTestId("recent-task-state-counts-loading-my_dag")).toBeInTheDocument();
    expect(screen.queryByTestId("recent-task-state-counts-my_dag")).toBeNull();
  });

  it("renders nothing for a Dag without runs", () => {
    renderCounts(undefined);
    expect(screen.queryByTestId("recent-task-state-counts-my_dag")).toBeNull();
  });

  it("renders one clickable badge per present state, linking to the counted runs' filtered task list", () => {
    renderCounts(makeEntry({ failed: 2, running: 1, success: 7 }));
    expect(screen.getByTestId("recent-task-state-counts-my_dag")).toBeInTheDocument();

    const failedLink = screen.getByTestId("recent-task-state-count-failed-my_dag");
    const runningLink = screen.getByTestId("recent-task-state-count-running-my_dag");
    const successLink = screen.getByTestId("recent-task-state-count-success-my_dag");

    expect(failedLink).toHaveAttribute("href", taskListLink("run_1", "failed"));
    expect(runningLink).toHaveAttribute("href", taskListLink("run_1", "running"));
    expect(successLink).toHaveAttribute("href", taskListLink("run_1", "success"));

    expect(failedLink).toHaveTextContent("2");
    expect(runningLink).toHaveTextContent("1");
    expect(successLink).toHaveTextContent("7");
  });

  it("omits absent and zero-count states instead of rendering empty badges", () => {
    renderCounts(makeEntry({ queued: 0, success: 3 }));
    expect(screen.getByTestId("recent-task-state-count-success-my_dag")).toBeInTheDocument();
    expect(screen.queryByTestId("recent-task-state-count-queued-my_dag")).toBeNull();
    expect(screen.queryByTestId("recent-task-state-count-failed-my_dag")).toBeNull();
  });

  it("maps no_status to the 'none' task filter value", () => {
    renderCounts(makeEntry({ no_status: 2, success: 1 }));
    const noStatusLink = screen.getByTestId("recent-task-state-count-no_status-my_dag");

    expect(noStatusLink).toHaveAttribute("href", taskListLink("run_1", "none"));
    expect(noStatusLink).toHaveTextContent("2");
  });

  it("links every counted run, keeping run id characters intact", () => {
    const runIds = ["manual__2026-10-02T01:32:39+00:00", "scheduled__2026-10-02T00:00:00+00:00"];

    renderCounts(makeEntry({ failed: 3 }, runIds));
    const href = screen.getByTestId("recent-task-state-count-failed-my_dag").getAttribute("href") ?? "";
    const params = new URL(href, "http://localhost").searchParams;

    expect(params.get("dag_id_pattern")).toBe("my_dag");
    expect(params.get("run_id_pattern")).toBe(runIds.join("|"));
    expect(params.get("task_state")).toBe("failed");
  });
});
