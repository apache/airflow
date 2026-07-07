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

import type { DAGLatestRunTaskInstanceStateCountsResponse } from "openapi/requests/types.gen";
import { BaseWrapper } from "src/utils/Wrapper";

import "../../i18n/config";
import { LatestRunTaskStateCounts } from "./LatestRunTaskStateCounts";

const renderCounts = (
  entry: DAGLatestRunTaskInstanceStateCountsResponse | undefined,
  options: { isLoading?: boolean } = {},
) =>
  render(<LatestRunTaskStateCounts dagId="my_dag" entry={entry} isLoading={options.isLoading ?? false} />, {
    wrapper: ({ children }) => (
      <BaseWrapper>
        <MemoryRouter>{children}</MemoryRouter>
      </BaseWrapper>
    ),
  });

const makeEntry = (stateCounts: Record<string, number>): DAGLatestRunTaskInstanceStateCountsResponse => ({
  dag_id: "my_dag",
  run_id: "run_1",
  state_counts: stateCounts,
});

describe("LatestRunTaskStateCounts", () => {
  it("renders skeleton placeholders while loading", () => {
    renderCounts(undefined, { isLoading: true });
    expect(screen.getByTestId("latest-run-task-state-counts-loading-my_dag")).toBeInTheDocument();
    expect(screen.queryByTestId("latest-run-task-state-counts-my_dag")).toBeNull();
  });

  it("renders nothing for a Dag without runs", () => {
    renderCounts(undefined);
    expect(screen.queryByTestId("latest-run-task-state-counts-my_dag")).toBeNull();
  });

  it("renders one clickable badge per present state, linking to the run's filtered task list", () => {
    renderCounts(makeEntry({ failed: 2, running: 1, success: 7 }));
    expect(screen.getByTestId("latest-run-task-state-counts-my_dag")).toBeInTheDocument();

    const failedLink = screen.getByTestId("latest-run-task-state-count-failed-my_dag");
    const runningLink = screen.getByTestId("latest-run-task-state-count-running-my_dag");
    const successLink = screen.getByTestId("latest-run-task-state-count-success-my_dag");

    expect(failedLink).toHaveAttribute("href", "/dags/my_dag/runs/run_1?task_state=failed");
    expect(runningLink).toHaveAttribute("href", "/dags/my_dag/runs/run_1?task_state=running");
    expect(successLink).toHaveAttribute("href", "/dags/my_dag/runs/run_1?task_state=success");

    expect(failedLink).toHaveTextContent("2");
    expect(runningLink).toHaveTextContent("1");
    expect(successLink).toHaveTextContent("7");
  });

  it("omits absent and zero-count states instead of rendering empty badges", () => {
    renderCounts(makeEntry({ queued: 0, success: 3 }));
    expect(screen.getByTestId("latest-run-task-state-count-success-my_dag")).toBeInTheDocument();
    expect(screen.queryByTestId("latest-run-task-state-count-queued-my_dag")).toBeNull();
    expect(screen.queryByTestId("latest-run-task-state-count-failed-my_dag")).toBeNull();
  });

  it("maps no_status to the 'none' task filter value", () => {
    renderCounts(makeEntry({ no_status: 2, success: 1 }));
    const noStatusLink = screen.getByTestId("latest-run-task-state-count-no_status-my_dag");

    expect(noStatusLink).toHaveAttribute("href", "/dags/my_dag/runs/run_1?task_state=none");
    expect(noStatusLink).toHaveTextContent("2");
  });
});
