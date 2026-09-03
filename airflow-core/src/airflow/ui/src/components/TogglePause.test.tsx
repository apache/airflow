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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TogglePause } from "./TogglePause";

const mocks = vi.hoisted(() => ({ isPending: false, mutate: vi.fn(), requireConfirmation: false }));

vi.mock("src/queries/useConfig", () => ({ useConfig: () => mocks.requireConfirmation }));
vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({ isPending: mocks.isPending, mutate: mocks.mutate }),
}));

afterEach(() => {
  cleanup();
  mocks.isPending = false;
  mocks.mutate.mockReset();
  mocks.requireConfirmation = false;
});

describe("TogglePause", () => {
  it("opens the drain-or-pause choice when flipping off an active Dag with unfinished runs", async () => {
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        hasUnfinishedRuns
        isPaused={false}
        schedulingState="active"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("drain-dag"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "draining" },
    });
  });

  it("pauses now from the drain-or-pause choice", async () => {
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        hasUnfinishedRuns
        isPaused={false}
        schedulingState="active"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("pause-dag-now"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "paused" },
    });
  });

  it("pauses immediately with no choice when the Dag has no unfinished runs", async () => {
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        hasUnfinishedRuns={false}
        isPaused={false}
        schedulingState="active"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));

    expect(screen.queryByTestId("drain-dag")).not.toBeInTheDocument();
    await waitFor(() =>
      expect(mocks.mutate).toHaveBeenCalledWith({
        dagId: "example",
        requestBody: { scheduling_state: "paused" },
      }),
    );
  });

  it("cancels draining with no choice when flipping on a draining Dag", async () => {
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        isPaused={false}
        schedulingState="draining"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));

    await waitFor(() =>
      expect(mocks.mutate).toHaveBeenCalledWith({
        dagId: "example",
        requestBody: { scheduling_state: "active" },
      }),
    );
  });

  it("shows the drain-or-pause choice even when confirmation is required", async () => {
    mocks.requireConfirmation = true;
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        hasUnfinishedRuns
        isPaused={false}
        schedulingState="active"
      />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));

    expect(await screen.findByTestId("drain-dag")).toBeInTheDocument();
    expect(mocks.mutate).not.toHaveBeenCalled();
  });

  it("renders a draining Dag's switch as unchecked", () => {
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        isPaused={false}
        schedulingState="draining"
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByTestId("toggle-pause")).not.toBeChecked();
  });
});
