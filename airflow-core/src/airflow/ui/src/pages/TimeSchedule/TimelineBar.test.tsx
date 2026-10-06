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
import type { ReactNode } from "react";

import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { TimelineBar } from "./TimelineBar";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => (key === "dagRun" ? "Dag Run" : key.replace("states.", "")),
  }),
}));

vi.mock("src/components/StateIcon", () => ({
  StateIcon: ({
    color,
    size,
    state,
  }: {
    readonly color?: string;
    readonly size?: number;
    readonly state?: string;
  }) => <svg data-color={color} data-size={size} data-state={state ?? "none"} data-testid="state-icon" />,
}));

vi.mock("src/system-components", () => ({
  Tooltip: ({ children }: { readonly children: ReactNode }) => children,
}));

const item: TimeScheduleItem = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "run-1",
  duration_ms: 60_000,
  end_date: "2024-01-01T00:01:00Z",
  is_placeholder: false,
  is_planned: false,
  is_time_scheduled: true,
  run_count: 1,
  start_date: "2024-01-01T00:00:00Z",
  state: "success",
};

const stateIconCases: Array<readonly [TimeScheduleItem["state"], string]> = [
  ["success", "success"],
  ["failed", "failed"],
  ["planned", "scheduled"],
  ["placeholder", "none"],
];

const renderTimelineBar = (overrides: Partial<TimeScheduleItem> = {}) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <MemoryRouter>
        <TimelineBar
          height="12px"
          item={{ ...item, ...overrides }}
          left="0"
          renderTooltip={() => null}
          testId="timeline-bar"
          width="64px"
        />
      </MemoryRouter>
    </ChakraProvider>,
  );

describe("TimelineBar", () => {
  it.each([
    [54_000, "54s"],
    [60_000, "1m"],
    [6 * 60_000 + 54_000, "6m"],
    [3_600_000 + 54_000, "1h"],
    [3_600_000 + 2 * 60_000 + 10_000, "1h 2m"],
  ])("formats %s ms without seconds at or above one minute", (durationMs, label) => {
    renderTimelineBar({ duration_ms: durationMs });

    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("paints the full time-proportional width for a planned bar without external padding", () => {
    renderTimelineBar({ duration_ms: 3_600_000, is_planned: true, state: "planned" });

    expect(screen.getByTestId("timeline-bar")).toHaveStyle({ paddingInline: "0", width: "64px" });
  });
  it.each(stateIconCases)("renders the %s state icon", (state, expectedIconState) => {
    renderTimelineBar({
      is_placeholder: state === "placeholder",
      is_planned: state === "planned",
      state,
    });

    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-state", expectedIconState);
  });

  it("names the state for assistive technology", () => {
    renderTimelineBar();

    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-color", "currentColor");
    expect(screen.getByRole("link", { name: "example_dag: success, 1 Dag Run" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );
  });
});
