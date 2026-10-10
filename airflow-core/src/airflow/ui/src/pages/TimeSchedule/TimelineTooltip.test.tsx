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
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { describe, expect, it, vi } from "vitest";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { TimelineTooltip } from "./TimelineTooltip";

dayjs.extend(utc);

vi.mock("src/components/StateIcon", () => ({
  StateIcon: ({ color, state }: { readonly color?: string; readonly state?: string }) => (
    <svg data-color={color} data-state={state ?? "none"} data-testid="state-icon" />
  ),
}));

const { translate } = vi.hoisted(() => ({
  translate: (key: string, options?: { count?: number }) =>
    key === "states.success" ? "Success" : options?.count === 1 ? "Dag Run" : "Dag Runs",
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ ...Object.fromEntries([["t", translate]]), i18n: { language: "en" } }),
}));

const item: TimeScheduleItem = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "run-1",
  duration_ms: 60_000,
  end_date: "2024-01-01T00:01:00Z",
  is_time_scheduled: true,
  run_after_max: "2024-01-01T00:00:00Z",
  run_after_min: "2024-01-01T00:00:00Z",
  run_count: 1,
  start_date: "2024-01-01T00:00:00Z",
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
    expect(screen.getByText("1 Dag Run")).toBeInTheDocument();
  });
});
