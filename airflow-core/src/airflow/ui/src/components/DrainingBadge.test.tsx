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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { DrainingBadge } from "./DrainingBadge";

const mocks = vi.hoisted(() => ({ isPending: false, mutate: vi.fn() }));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({ isPending: mocks.isPending, mutate: mocks.mutate }),
}));

const openPopover = () => fireEvent.click(screen.getByTestId("draining-badge"));

afterEach(() => {
  mocks.mutate.mockReset();
  mocks.isPending = false;
});

describe("DrainingBadge", () => {
  it("renders the draining state", () => {
    render(<DrainingBadge dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.getByTestId("draining-badge")).toBeInTheDocument();
  });

  it("keeps the drain actions behind the popover", async () => {
    render(<DrainingBadge dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.queryByTestId("banner-cancel-drain")).not.toBeInTheDocument();

    openPopover();

    await waitFor(() => expect(screen.getByTestId("banner-cancel-drain")).toBeInTheDocument());
    expect(screen.getByTestId("banner-pause-now")).toBeInTheDocument();
  });

  it.each([
    ["banner-cancel-drain", "active"],
    ["banner-pause-now", "paused"],
  ] as const)("%s sets scheduling_state to %s", async (testId, schedulingState) => {
    render(<DrainingBadge dagId="example_dag" />, { wrapper: Wrapper });
    openPopover();
    await waitFor(() => expect(screen.getByTestId(testId)).toBeInTheDocument());
    fireEvent.click(screen.getByTestId(testId));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { scheduling_state: schedulingState },
    });
  });

  it("omits the actions without a dagId, as in list views", async () => {
    render(<DrainingBadge />, { wrapper: Wrapper });
    openPopover();

    // Wait for the popover body, so the assertions below cannot pass merely because it never opened.
    await waitFor(() => expect(screen.getByTestId("draining-explanation")).toBeInTheDocument());

    expect(screen.queryByTestId("banner-cancel-drain")).not.toBeInTheDocument();
    expect(screen.queryByTestId("banner-pause-now")).not.toBeInTheDocument();
  });
});
