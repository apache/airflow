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

import type { TaskInstanceRetryDetails } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { RetryDetails } from "./RetryDetails";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => {
      const translations: Record<string, string> = {
        "taskInstance.retry.backoffDelay": "Exponential backoff delay",
        "taskInstance.retry.capped": "capped",
        "taskInstance.retry.jitter": "Calculated jitter",
        "taskInstance.retry.sources.retry_policy": "Retry policy",
        "taskInstance.retry.sources.task_configuration": "Task configuration",
        "taskInstance.retry.title": "Retry Details",
      };

      return translations[key] ?? key;
    },
  }),
}));

const retryDetails: TaskInstanceRetryDetails = {
  backoff_delay_seconds: 360,
  configured_delay_seconds: 180,
  delay_seconds: 678,
  eligible_at: "2026-08-27T07:44:20Z",
  is_capped: false,
  jitter_seconds: 318,
  maximum_delay_seconds: 900,
  reason: null,
  source: "task_configuration",
};

describe("RetryDetails", () => {
  it("shows the retry time and deterministic jitter breakdown", () => {
    render(<RetryDetails details={retryDetails} />, { wrapper: Wrapper });

    expect(screen.getByText("Retry Details")).toBeInTheDocument();
    expect(screen.getByTestId("time-display")).toHaveAttribute("datetime", "2026-08-27T07:44:20Z");
    expect(screen.getByText("11m 18s")).toBeInTheDocument();
    expect(screen.getByText("3m")).toBeInTheDocument();
    expect(screen.getByText("6m")).toBeInTheDocument();
    expect(screen.getByText("5m 18s")).toBeInTheDocument();
    expect(screen.getByText("15m")).toBeInTheDocument();
    expect(screen.getByText("Task configuration")).toBeInTheDocument();
    expect(screen.getByText("Calculated jitter")).toBeInTheDocument();
  });

  it("marks a retry delay that was capped", () => {
    render(<RetryDetails details={{ ...retryDetails, is_capped: true }} />, { wrapper: Wrapper });

    expect(screen.getByText("11m 18s (capped)")).toBeInTheDocument();
  });

  it("shows retry policy context without a backoff breakdown", () => {
    render(
      <RetryDetails
        details={{
          ...retryDetails,
          backoff_delay_seconds: null,
          configured_delay_seconds: null,
          jitter_seconds: null,
          maximum_delay_seconds: null,
          reason: "Rate limit",
          source: "retry_policy",
        }}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("Retry policy")).toBeInTheDocument();
    expect(screen.getByText("Rate limit")).toBeInTheDocument();
    expect(screen.queryByText("Calculated jitter")).not.toBeInTheDocument();
    expect(screen.queryByText("Exponential backoff delay")).not.toBeInTheDocument();
    expect(screen.queryByText("taskInstance.retry.configuredDelay")).not.toBeInTheDocument();
  });
});
