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
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { DagRunMetrics } from "./DagRunMetrics";

describe("DagRunMetrics", () => {
  it("shows percentages when counts are not capped", () => {
    render(
      <DagRunMetrics
        dagRunStates={{ failed: 0, queued: 1, running: 0, success: 3 }}
        startDate="2026-01-01T00:00:00Z"
        stateCountLimit={1000}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("25.00%")).toBeInTheDocument();
    expect(screen.getByText("75.00%")).toBeInTheDocument();
  });

  it("hides percentages when any state reaches the API cap", () => {
    render(
      <DagRunMetrics
        dagRunStates={{ failed: 7, queued: 0, running: 0, success: 1000 }}
        startDate="2026-01-01T00:00:00Z"
        stateCountLimit={1000}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("1000+")).toBeInTheDocument();
    expect(screen.queryByText("99.30%")).not.toBeInTheDocument();
    expect(screen.queryByText("0.70%")).not.toBeInTheDocument();
  });
});
