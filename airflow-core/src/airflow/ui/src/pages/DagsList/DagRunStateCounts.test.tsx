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

import { BaseWrapper } from "src/utils/Wrapper";

import "../../i18n/config";
import { DagRunStateCounts } from "./DagRunStateCounts";

const renderCounts = (counts: Record<string, number> | undefined, options: { isLoading?: boolean } = {}) =>
  render(<DagRunStateCounts counts={counts} dagId="my_dag" isLoading={options.isLoading ?? false} />, {
    wrapper: ({ children }) => (
      <BaseWrapper>
        <MemoryRouter>{children}</MemoryRouter>
      </BaseWrapper>
    ),
  });

describe("DagRunStateCounts", () => {
  it("renders skeleton placeholders while loading", () => {
    renderCounts(undefined, { isLoading: true });
    expect(screen.getByTestId("run-state-counts-loading-my_dag")).toBeInTheDocument();
    expect(screen.queryByTestId("run-state-counts-my_dag")).toBeNull();
  });

  it("renders one clickable badge per state with counts", () => {
    renderCounts({ failed: 3, queued: 0, running: 1, success: 42 });
    expect(screen.getByTestId("run-state-counts-my_dag")).toBeInTheDocument();

    const successLink = screen.getByTestId("run-state-count-success-my_dag");
    const failedLink = screen.getByTestId("run-state-count-failed-my_dag");
    const runningLink = screen.getByTestId("run-state-count-running-my_dag");
    const queuedLink = screen.getByTestId("run-state-count-queued-my_dag");

    expect(successLink).toHaveAttribute("href", "/dags/my_dag/runs?state=success");
    expect(failedLink).toHaveAttribute("href", "/dags/my_dag/runs?state=failed");
    expect(runningLink).toHaveAttribute("href", "/dags/my_dag/runs?state=running");
    expect(queuedLink).toHaveAttribute("href", "/dags/my_dag/runs?state=queued");

    expect(successLink).toHaveTextContent("42");
    expect(failedLink).toHaveTextContent("3");
    expect(runningLink).toHaveTextContent("1");
    expect(queuedLink).toHaveTextContent("0");
  });

  it("dims zero-count badges but keeps them clickable", () => {
    renderCounts({ failed: 0, queued: 0, running: 0, success: 5 });
    const queuedLink = screen.getByTestId("run-state-count-queued-my_dag");
    // The badge is the link's child Chakra Badge; check style is dimmed.
    const badge = queuedLink.querySelector('[data-testid="state-badge"]');

    expect(badge).not.toBeNull();
    expect(badge).toHaveStyle({ opacity: "0.4" });
    // Still a real link so operators can confirm "no failures" by clicking through.
    expect(queuedLink).toHaveAttribute("href", "/dags/my_dag/runs?state=queued");
  });

  it("treats missing keys in `counts` as zero", () => {
    renderCounts({ failed: 2 });
    expect(screen.getByTestId("run-state-count-success-my_dag")).toHaveTextContent("0");
    expect(screen.getByTestId("run-state-count-failed-my_dag")).toHaveTextContent("2");
  });

  it("renders large counts in compact notation", () => {
    renderCounts({ failed: 1_500_000, queued: 1234, running: 999, success: 100_000 });
    // ICU compact notation: 100K, 1.5M, 1.2K. Small numbers stay literal.
    expect(screen.getByTestId("run-state-count-success-my_dag")).toHaveTextContent("100K");
    expect(screen.getByTestId("run-state-count-failed-my_dag")).toHaveTextContent("1.5M");
    expect(screen.getByTestId("run-state-count-queued-my_dag")).toHaveTextContent("1.2K");
    // <1000 stays as-is.
    expect(screen.getByTestId("run-state-count-running-my_dag")).toHaveTextContent("999");
  });
});
