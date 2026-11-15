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
import { describe, expect, it, vi } from "vitest";

import type { LoopSummaryResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { useLoopOutcomeStats } from "./useLoopOutcomeStats";

const mockLoopSummary: { data?: Partial<LoopSummaryResponse> } = {};

vi.mock("src/queries/useLoopSummary", () => ({
  useLoopSummary: () => ({ data: mockLoopSummary.data }),
}));

const OutcomeStats = () => (
  <>
    {useLoopOutcomeStats({ dagId: "refine_estimate", groupId: "refine", runId: "run" }).map((stat) => (
      <div key={stat.label}>{stat.value}</div>
    ))}
  </>
);

describe("useLoopOutcomeStats", () => {
  it.each([
    { shown: true, status: "stopped_early" },
    { shown: true, status: "failed" },
    { shown: false, status: "running" },
  ] as const)("shows the final decision reason: $shown for a $status loop", ({ shown, status }) => {
    mockLoopSummary.data = {
      iterations: [
        { decision_reason: "error 0.25, needs < 1e-06", index: 0 },
        { decision_reason: "error 4.5e-12, needs < 1e-06", index: 1 },
      ],
      iterations_ran: 2,
      max_iterations: 10,
      status,
    };

    render(<OutcomeStats />, { wrapper: Wrapper });

    expect(screen.queryByText("error 4.5e-12, needs < 1e-06") !== null).toBe(shown);
    expect(screen.queryByText("error 0.25, needs < 1e-06")).not.toBeInTheDocument();
  });
});
