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
import { render, screen } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";

import { useXcomServiceGetXcomEntry } from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { XComEntry } from "./XComEntry";

// The component loads its value from the API, so mock the hook instead of running a server.
vi.mock("openapi/queries", () => ({
  useXcomServiceGetXcomEntry: vi.fn(),
}));

const renderEntry = (value: unknown) => {
  vi.mocked(useXcomServiceGetXcomEntry).mockReturnValue({
    data: { value },
    isLoading: false,
  } as never);

  render(<XComEntry dagId="dag" mapIndex={-1} runId="run" taskId="task" xcomKey="return_value" />, {
    wrapper: Wrapper,
  });
};

describe("XComEntry", () => {
  it("keeps newlines in string values", () => {
    // Regression: the value was split on all whitespace and re-joined with
    // single spaces, which turned newlines into spaces.
    renderEntry("a\nb");

    expect(screen.getByTestId("xcom-value").textContent).toContain("a\nb");
  });

  it("keeps newlines and still renders links", () => {
    renderEntry("Line 1\nSee https://airflow.apache.org\nLine 3");

    expect(screen.getByRole("link")).toHaveAttribute("href", "https://airflow.apache.org");
    expect(screen.getByTestId("xcom-value").textContent).toContain("Line 1\nSee ");
  });
});
