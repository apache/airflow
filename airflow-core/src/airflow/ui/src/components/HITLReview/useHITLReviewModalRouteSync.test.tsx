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
import { act, renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import type * as ReactRouterDom from "react-router-dom";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { useHITLReviewModalRouteSync } from "./useHITLReviewModalRouteSync";

const navigate = vi.hoisted(() => vi.fn());

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useNavigate: () => navigate,
  };
});

const createWrapper =
  (initialEntry = "/") =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

beforeEach(() => {
  navigate.mockClear();
});

describe("useHITLReviewModalRouteSync", () => {
  it("opens the modal when the route points to required actions", () => {
    const onClose = vi.fn();
    const onOpen = vi.fn();

    renderHook(() => useHITLReviewModalRouteSync({ onClose, onOpen }), {
      wrapper: createWrapper("/dags/example_dag/tasks/example_task/required_actions"),
    });

    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(onClose).not.toHaveBeenCalled();
  });

  it("removes the required actions route when closing the HITL review", () => {
    const onClose = vi.fn();
    const { result } = renderHook(() => useHITLReviewModalRouteSync({ onClose, onOpen: vi.fn() }), {
      wrapper: createWrapper("/dags/example_dag/tasks/example_task/required_actions"),
    });

    act(() => {
      result.current.onCloseHITLReview();
    });

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(navigate).toHaveBeenCalledWith("/dags/example_dag/tasks/example_task", { replace: true });
  });
});
