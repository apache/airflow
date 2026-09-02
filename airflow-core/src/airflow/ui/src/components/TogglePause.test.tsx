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
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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
  it("starts draining an active Dag", async () => {
    render(
      <TogglePause dagDisplayName="Example Dag" dagId="example" isPaused={false} schedulingState="active" />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("drain-dag"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "draining" },
    });
  });

  it("pauses an active Dag immediately", async () => {
    render(
      <TogglePause dagDisplayName="Example Dag" dagId="example" isPaused={false} schedulingState="active" />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("pause-dag-now"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "paused" },
    });
  });

  it("cancels draining a Dag", async () => {
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
    fireEvent.click(await screen.findByTestId("activate-dag"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "active" },
    });
  });

  it("pauses a draining Dag immediately", async () => {
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
    fireEvent.click(await screen.findByTestId("pause-dag-now"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "paused" },
    });
  });

  it("unpauses a paused Dag", async () => {
    render(<TogglePause dagDisplayName="Example Dag" dagId="example" isPaused schedulingState="paused" />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("activate-dag"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "active" },
    });
  });

  it("confirms draining before changing state when confirmation is required", async () => {
    mocks.requireConfirmation = true;
    render(
      <TogglePause dagDisplayName="Example Dag" dagId="example" isPaused={false} schedulingState="active" />,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByTestId("toggle-pause"));
    fireEvent.click(await screen.findByTestId("drain-dag"));

    expect(mocks.mutate).not.toHaveBeenCalled();
    expect(await screen.findByRole("dialog")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("confirmation-confirm-button"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example",
      requestBody: { scheduling_state: "draining" },
    });
  });

  it("stays disabled while a scheduling-state update is pending", () => {
    mocks.isPending = true;
    render(
      <TogglePause
        dagDisplayName="Example Dag"
        dagId="example"
        disabled={false}
        isPaused={false}
        schedulingState="active"
      />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByTestId("toggle-pause")).toBeDisabled();
  });
});
