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

import { TimelineBar } from "./TimelineBar";
import type { TimelineItem } from "./types";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key.replace("states.", ""),
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

const item: TimelineItem = {
  dagId: "example_dag",
  dagRunId: "run-1",
  durationMs: 60_000,
  endDate: "2024-01-01T00:01:00Z",
  isPlaceholder: false,
  isPlanned: false,
  isTimeScheduled: true,
  label: "example_dag",
  runCount: 1,
  startDate: "2024-01-01T00:00:00Z",
  state: "success",
};

const stateIconCases: Array<readonly [TimelineItem["state"], string]> = [
  ["success", "success"],
  ["failed", "failed"],
  ["planned", "scheduled"],
  ["placeholder", "none"],
];

const renderTimelineBar = (
  overrides: Partial<TimelineItem> = {},
  showDagLabel = false,
  labelLineClamp?: number,
) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <MemoryRouter>
        <TimelineBar
          height="12px"
          item={{ ...item, ...overrides }}
          labelLineClamp={labelLineClamp}
          left="0"
          renderTooltip={() => null}
          showDagLabel={showDagLabel}
          testId="timeline-bar"
          width="64px"
        />
      </MemoryRouter>
    </ChakraProvider>,
  );

describe("TimelineBar", () => {
  it.each(stateIconCases)("renders the %s state icon", (state, expectedIconState) => {
    renderTimelineBar({
      isPlaceholder: state === "placeholder",
      isPlanned: state === "planned",
      state,
    });

    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-state", expectedIconState);
  });

  it("names the state for assistive technology", () => {
    renderTimelineBar();

    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-color", "currentColor");
    expect(screen.getByRole("link", { name: "View Dag run run-1: success" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );
  });
});
