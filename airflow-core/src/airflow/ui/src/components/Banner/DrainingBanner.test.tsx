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
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { DagSchedulingState } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import DrainingBanner from "./DrainingBanner";

const mocks = vi.hoisted(() => ({ getDag: vi.fn(), isPending: false, mutate: vi.fn() }));

vi.mock("openapi/queries", () => ({
  useDagServiceGetDag: mocks.getDag,
}));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({ isPending: mocks.isPending, mutate: mocks.mutate }),
}));

const mockDag = (schedulingState: DagSchedulingState) => ({ scheduling_state: schedulingState });

afterEach(() => {
  mocks.getDag.mockReset();
  mocks.mutate.mockReset();
  mocks.isPending = false;
});

describe("DrainingBanner", () => {
  it.each(["active", "paused"] as const)("renders nothing when scheduling_state is %s", (state) => {
    mocks.getDag.mockReturnValue({ data: mockDag(state) });

    const { container } = render(<DrainingBanner dagId="example_dag" />, { wrapper: Wrapper });

    expect(container).toBeEmptyDOMElement();
  });

  it("renders the banner when the Dag is draining", () => {
    mocks.getDag.mockReturnValue({ data: mockDag("draining") });

    render(<DrainingBanner dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.getByTestId("banner-cancel-drain")).toBeInTheDocument();
    expect(screen.getByTestId("banner-pause-now")).toBeInTheDocument();
  });

  it("cancels draining", () => {
    mocks.getDag.mockReturnValue({ data: mockDag("draining") });

    render(<DrainingBanner dagId="example_dag" />, { wrapper: Wrapper });
    fireEvent.click(screen.getByTestId("banner-cancel-drain"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { scheduling_state: "active" },
    });
  });

  it("pauses now", () => {
    mocks.getDag.mockReturnValue({ data: mockDag("draining") });

    render(<DrainingBanner dagId="example_dag" />, { wrapper: Wrapper });
    fireEvent.click(screen.getByTestId("banner-pause-now"));

    expect(mocks.mutate).toHaveBeenCalledWith({
      dagId: "example_dag",
      requestBody: { scheduling_state: "paused" },
    });
  });
});
