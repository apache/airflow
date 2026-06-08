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
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TriagePanel } from "./TriagePanel";

const mockUseTriageSummary = vi.fn();

vi.mock("src/queries/useTriageSummary", () => ({
  useTriageSummary: (...args: unknown[]) => mockUseTriageSummary(...args),
}));

describe("TriagePanel", () => {
  it("renders loading skeleton without blocking layout", () => {
    mockUseTriageSummary.mockReturnValue({ data: undefined, isLoading: true });

    render(
      <Wrapper>
        <TriagePanel dagId="sample_dag" mapIndex={-1} runId="manual__1" taskId="fail_task" />
      </Wrapper>,
    );

    expect(screen.getByTestId("triage-panel")).toBeInTheDocument();
    expect(screen.getByText("Failure triage")).toBeInTheDocument();
  });

  it("renders failure category, summary, and remediation checklist", () => {
    mockUseTriageSummary.mockReturnValue({
      data: {
        categories: [{ confidence: 0.8, name: "CODE" }],
        error: null,
        log_available: true,
        remediations: [
          {
            doc_links: ["https://airflow.apache.org/docs/"],
            steps: ["Inspect the traceback", "Fix the operator code"],
            title: "Fix Python error",
          },
        ],
        root_cause_summary: "Likely Code failure (80% confidence). Last error: AttributeError",
      },
      isLoading: false,
    });

    render(
      <Wrapper>
        <TriagePanel dagId="sample_dag" mapIndex={-1} runId="manual__1" taskId="fail_task" />
      </Wrapper>,
    );

    expect(screen.getByText("Failure category")).toBeInTheDocument();
    expect(screen.getByText("CODE (80%)")).toBeInTheDocument();
    expect(screen.getByText(/Likely Code failure/)).toBeInTheDocument();
    expect(screen.getByText("Remediation checklist")).toBeInTheDocument();
    expect(screen.getByText("Fix Python error")).toBeInTheDocument();
    expect(screen.getByText("Inspect the traceback")).toBeInTheDocument();
  });

  it("renders unavailable message when summary is missing", () => {
    mockUseTriageSummary.mockReturnValue({ data: null, isLoading: false });

    render(
      <Wrapper>
        <TriagePanel dagId="sample_dag" mapIndex={-1} runId="manual__1" taskId="fail_task" />
      </Wrapper>,
    );

    expect(screen.getByText("Triage summary is not available for this task.")).toBeInTheDocument();
  });
});
