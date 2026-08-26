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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { Wrapper } from "src/utils/Wrapper";

import { GridButton } from "./GridButton";

describe("GridButton", () => {
  it("shows run details in the grid tooltip respecting the selected timezone", () => {
    vi.useFakeTimers();

    render(
      <TimezoneContext.Provider value={{ selectedTimezone: "Europe/Vienna", setSelectedTimezone: vi.fn() }}>
        <GridButton
          dagId="example_dag"
          duration={3661}
          runAfter="2026-04-21T00:00:00+00:00"
          runId="manual__2026-04-21T00:00:00+00:00"
          searchParams=""
          state="success"
        >
          <span>bar</span>
        </GridButton>
      </TimezoneContext.Provider>,
      { wrapper: Wrapper },
    );

    act(() => {
      fireEvent.mouseEnter(screen.getByText("bar"));
      vi.advanceTimersByTime(500);
    });

    expect(screen.getByTestId("basic-tooltip")).toHaveTextContent("2026-04-21 02:00:00");
    expect(screen.getByTestId("basic-tooltip")).toHaveTextContent(
      "common:runId: manual__2026-04-21T00:00:00+00:00",
    );
    expect(screen.getByTestId("basic-tooltip")).toHaveTextContent("duration: 01:01:01");

    vi.useRealTimers();
  });
});
