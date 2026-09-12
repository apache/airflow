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
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { TimelineTooltip } from "./TimelineTooltip";
import type { TimelineItem } from "./types";

vi.mock("src/components/StateIcon", () => ({
  StateIcon: ({ color, state }: { readonly color?: string; readonly state?: string }) => (
    <svg data-color={color} data-state={state ?? "none"} data-testid="state-icon" />
  ),
}));

const { translate } = vi.hoisted(() => ({
  translate: (key: string, options?: { count?: number }) =>
    key === "states.success" ? "Success" : `${options?.count ?? ""} Dag runs`.trim(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => Object.fromEntries([["t", translate]]),
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

describe("TimelineTooltip", () => {
  it("renders the item state and details", () => {
    render(
      <ChakraProvider value={defaultSystem}>
        <TimelineTooltip item={item} selectedTimezone="UTC" />
      </ChakraProvider>,
    );

    expect(screen.getByText("example_dag")).toBeInTheDocument();
    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-color", "currentColor");
    expect(screen.getByTestId("state-icon")).toHaveAttribute("data-state", "success");
    expect(screen.getByText("Success")).toBeInTheDocument();
    expect(screen.getByText("00:00 – 00:01")).toBeInTheDocument();
    expect(screen.getByText("1 Dag runs")).toBeInTheDocument();
  });
});
