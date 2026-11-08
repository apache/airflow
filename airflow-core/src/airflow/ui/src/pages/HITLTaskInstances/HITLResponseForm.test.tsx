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
/// <reference types="@testing-library/jest-dom" />
import { QueryClient } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, renderHook, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests/services.gen";
import type { HITLDetailHistory, TaskInstanceHistoryResponse } from "openapi/requests/types.gen";

import { toaster } from "src/system-components";

import type { useUpdateHITLDetail as UseUpdateHITLDetail } from "src/queries/useUpdateHITLDetail";
import { Wrapper } from "src/utils/Wrapper";

import { HITLResponseForm } from "./HITLResponseForm";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

const { updateHITLResponse } = vi.hoisted(() => ({ updateHITLResponse: vi.fn() }));

vi.mock("src/queries/useUpdateHITLDetail", () => ({
  useUpdateHITLDetail: () => ({ updateHITLResponse }),
}));

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------
const MOCK_TASK_INSTANCE = {
  dag_id: "test_dag",
  dag_run_id: "run_1",
  map_index: -1,
  state: "deferred",
  task_id: "test_task",
} as TaskInstanceHistoryResponse;

const makeHITLDetail = (
  options: Array<string>,
  overrides: Partial<HITLDetailHistory> = {},
): { task_instance: TaskInstanceHistoryResponse } & Omit<HITLDetailHistory, "task_instance"> => ({
  assigned_users: [],
  body: "Please pick one.",
  chosen_options: [],
  created_at: new Date().toISOString(),
  defaults: null,
  multiple: false,
  options,
  params: {},
  params_input: {},
  responded_at: null,
  responded_by_user: null,
  response_received: false,
  subject: "Test subject",
  task_instance: MOCK_TASK_INSTANCE,
  ...overrides,
});

const renderForm = (options: Array<string>, overrides?: Partial<HITLDetailHistory>) =>
  render(<HITLResponseForm hitlDetail={makeHITLDetail(options, overrides)} />, {
    wrapper: Wrapper,
  });

// ---------------------------------------------------------------------------
// Tests — option-button rendering boundary
//
// HITLResponseForm renders options one of two ways:
//   shouldRenderOptionButton=true  → one Button per option (data-testid="hitl-option-<n>")
//   shouldRenderOptionButton=false → a generic "Respond" button, no per-option buttons
//
// Bug (#64413): condition was `options.length < 4`, so with exactly 4 options
// shouldRenderOptionButton was false and the footer rendered nothing at all.
// Fix: change to `options.length <= 4`.
// ---------------------------------------------------------------------------
describe("HITLResponseForm – option button rendering boundary", () => {
  it("renders per-option buttons for 1 option", () => {
    renderForm(["Only"]);
    expect(screen.getByTestId("hitl-option-Only")).toBeInTheDocument();
  });

  it("renders per-option buttons for 2 options", () => {
    renderForm(["Yes", "No"]);
    expect(screen.getByTestId("hitl-option-Yes")).toBeInTheDocument();
    expect(screen.getByTestId("hitl-option-No")).toBeInTheDocument();
  });

  it("renders per-option buttons for 3 options", () => {
    const opts = ["Creator", "Explorer", "Viewer"];

    renderForm(opts);
    for (const opt of opts) {
      expect(screen.getByTestId(`hitl-option-${opt}`)).toBeInTheDocument();
    }
  });

  // Regression test for #64413 — exactly 4 options previously rendered nothing.
  it("renders per-option buttons for exactly 4 options", () => {
    const opts = ["Creator", "Explorer", "ExplorerCanPublish", "Viewer"];

    renderForm(opts);
    for (const opt of opts) {
      expect(screen.getByTestId(`hitl-option-${opt}`)).toBeInTheDocument();
    }
  });

  it("does NOT render per-option buttons for 5 options", () => {
    const opts = ["A", "B", "C", "D", "E"];

    renderForm(opts);
    for (const opt of opts) {
      expect(screen.queryByTestId(`hitl-option-${opt}`)).not.toBeInTheDocument();
    }
  });

  it("does NOT render per-option buttons when multiple=true", () => {
    renderForm(["A", "B"], { multiple: true });
    expect(screen.queryByTestId("hitl-option-A")).not.toBeInTheDocument();
    expect(screen.queryByTestId("hitl-option-B")).not.toBeInTheDocument();
  });

  it("renders Approve and Reject buttons for a 2-option approval task", () => {
    renderForm(["Approve", "Reject"]);
    expect(screen.getByTestId("hitl-option-Approve")).toBeInTheDocument();
    expect(screen.getByTestId("hitl-option-Reject")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Tests: pending-state enablement
//
// HITL tasks park in "deferred" (pre-3.3) or "awaiting_input" (from 3.3). Both
// are "pending a response", so the option buttons must be enabled in either
// state. Previously the form only treated "deferred" as pending, which disabled
// the response form for awaiting_input tasks.
// ---------------------------------------------------------------------------
describe("HITLResponseForm – pending-state enablement", () => {
  it("enables option buttons for a deferred task", () => {
    renderForm(["Yes", "No"], {
      task_instance: { ...MOCK_TASK_INSTANCE, state: "deferred" },
    });
    expect(screen.getByTestId("hitl-option-Yes")).toBeEnabled();
    expect(screen.getByTestId("hitl-option-No")).toBeEnabled();
  });

  it("enables option buttons for an awaiting_input task", () => {
    renderForm(["Yes", "No"], {
      task_instance: { ...MOCK_TASK_INSTANCE, state: "awaiting_input" },
    });
    expect(screen.getByTestId("hitl-option-Yes")).toBeEnabled();
    expect(screen.getByTestId("hitl-option-No")).toBeEnabled();
  });

  it("disables option buttons for a finished task", () => {
    renderForm(["Yes", "No"], {
      task_instance: { ...MOCK_TASK_INSTANCE, state: "success" },
    });
    expect(screen.getByTestId("hitl-option-Yes")).toBeDisabled();
    expect(screen.getByTestId("hitl-option-No")).toBeDisabled();
  });
});

describe("HITLResponseForm – the answer names the request it was given for", () => {
  it("sends the created_at of the request the form was loaded for", () => {
    updateHITLResponse.mockClear();
    const loaded = "2026-10-14T01:00:00.123456Z";

    renderForm(["Approve", "Reject"], { created_at: loaded });
    fireEvent.click(screen.getByTestId("hitl-option-Approve"));

    expect(updateHITLResponse).toHaveBeenCalledWith(
      expect.objectContaining({ chosen_options: ["Approve"], request_created_at: loaded }),
    );
  });

  it("keeps the loaded created_at when the request under an open form is replaced", () => {
    updateHITLResponse.mockClear();
    const loaded = "2026-10-14T01:00:00.123456Z";
    const { rerender } = renderForm(["Approve", "Reject"], { created_at: loaded });

    // The task asked again while the form stayed open; a polled refetch hands the form the new request.
    rerender(
      <HITLResponseForm
        hitlDetail={makeHITLDetail(["Approve", "Reject"], { created_at: "2026-10-14T01:05:00.000000Z" })}
      />,
    );
    fireEvent.click(screen.getByTestId("hitl-option-Approve"));

    expect(updateHITLResponse).toHaveBeenCalledWith(expect.objectContaining({ request_created_at: loaded }));
  });
});

const selectOption = (option: string) => {
  fireEvent.keyDown(screen.getByRole("combobox"), { key: "ArrowDown" });
  fireEvent.click(screen.getByText(option));
};

const clearLastSelection = () => {
  fireEvent.keyDown(screen.getByRole("combobox"), { key: "Backspace" });
};

describe("HITLResponseForm – multiple requires a selection", () => {
  it("disables Respond and shows the hint when multiple=true and nothing is selected", () => {
    renderForm(["A", "B", "C", "D", "E"], { multiple: true });

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeDisabled();
    expect(screen.getByText("response.selectAtLeastOne")).toBeInTheDocument();
  });

  it("disables Respond and shows the hint for multiple=true with 2 options", () => {
    renderForm(["A", "B"], { multiple: true });

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeDisabled();
    expect(screen.getByText("response.selectAtLeastOne")).toBeInTheDocument();
  });

  it("enables Respond and hides the hint once an option is selected", () => {
    renderForm(["A", "B", "C", "D", "E"], { multiple: true });
    selectOption("B");

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeEnabled();
    expect(screen.queryByText("response.selectAtLeastOne")).not.toBeInTheDocument();
  });

  it("sends the selected options when Respond is clicked", () => {
    updateHITLResponse.mockClear();
    renderForm(["A", "B", "C", "D", "E"], { multiple: true });
    selectOption("B");
    fireEvent.click(screen.getByRole("button", { name: /response\.respond/u }));

    expect(updateHITLResponse).toHaveBeenCalledWith(expect.objectContaining({ chosen_options: ["B"] }));
  });

  it("does not submit an empty chosen_options", () => {
    updateHITLResponse.mockClear();
    renderForm(["A", "B", "C", "D", "E"], { multiple: true });
    fireEvent.click(screen.getByRole("button", { name: /response\.respond/u }));

    expect(updateHITLResponse).not.toHaveBeenCalled();
  });

  it("re-disables Respond after the last selection is cleared", () => {
    renderForm(["A", "B", "C", "D", "E"], { multiple: true });
    selectOption("B");
    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeEnabled();

    clearLastSelection();

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeDisabled();
    expect(screen.getByText("response.selectAtLeastOne")).toBeInTheDocument();
  });

  it("does not show the hint when defaults preselect an option", () => {
    renderForm(["A", "B", "C", "D", "E"], { defaults: ["A"], multiple: true });

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeEnabled();
    expect(screen.queryByText("response.selectAtLeastOne")).not.toBeInTheDocument();
  });

  it("disables Respond and shows the hint when multiple=false with 5 options and nothing is selected", () => {
    renderForm(["A", "B", "C", "D", "E"]);

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeDisabled();
    expect(screen.getByText("response.selectAtLeastOne")).toBeInTheDocument();
  });

  it("does not block a single-select Respond when defaults preselect an option", () => {
    renderForm(["A", "B", "C", "D", "E"], { defaults: ["A"] });

    expect(screen.getByRole("button", { name: /response\.respond/u })).toBeEnabled();
    expect(screen.queryByText("response.selectAtLeastOne")).not.toBeInTheDocument();
  });

  it("keeps option buttons enabled for 4 options or fewer", () => {
    renderForm(["A", "B", "C", "D"]);

    expect(screen.getByTestId("hitl-option-A")).toBeEnabled();
    expect(screen.queryByText("response.selectAtLeastOne")).not.toBeInTheDocument();
  });
});

const submitRejectedAnswer = async (rejection: unknown) => {
  const { useUpdateHITLDetail: useRealUpdateHITLDetail } = await vi.importActual<{
    useUpdateHITLDetail: typeof UseUpdateHITLDetail;
  }>("src/queries/useUpdateHITLDetail");

  vi.spyOn(TaskInstanceService, "updateHitlDetail").mockRejectedValue(rejection);
  const createToast = vi.spyOn(toaster, "create").mockImplementation(() => "toast-id");
  const invalidateQueries = vi.spyOn(QueryClient.prototype, "invalidateQueries");
  const { result } = renderHook(
    () =>
      useRealUpdateHITLDetail({ dagId: "test_dag", dagRunId: "run_1", mapIndex: -1, taskId: "test_task" }),
    { wrapper: Wrapper },
  );

  result.current.updateHITLResponse({ chosen_options: ["Approve"], params_input: {} });

  return { createToast, invalidateQueries };
};

describe("useUpdateHITLDetail – a refused answer", () => {
  const refusalDetail =
    "The Task Instance is no longer waiting for a response, so this Human-in-the-loop response was not recorded.";

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("shows the reason the backend gave instead of the bare status text", async () => {
    const { createToast } = await submitRejectedAnswer({
      body: { detail: refusalDetail },
      message: "Conflict",
      status: 409,
    });

    await waitFor(() =>
      expect(createToast).toHaveBeenCalledWith(
        expect.objectContaining({ description: refusalDetail, type: "error" }),
      ),
    );
  });

  it.each([
    {
      detail: [
        { loc: ["body", "chosen_options"], msg: "Field required", type: "missing" },
        { loc: ["body", "params_input", "name"], msg: "Input should be a string", type: "string_type" },
      ],
      expected: "body.chosen_options Field required, body.params_input.name Input should be a string",
      name: "validation entries",
    },
    {
      detail: { reason: "stale request" },
      expected: "reason: stale request",
      name: "a mapping",
    },
  ])("joins $name detail into one description", async ({ detail, expected }) => {
    const { createToast } = await submitRejectedAnswer({
      body: { detail },
      message: "Unprocessable Entity",
      status: 422,
    });

    await waitFor(() =>
      expect(createToast).toHaveBeenCalledWith({
        description: expected,
        title: "response.error",
        type: "error",
      }),
    );
  });

  it("falls back to the error message when the backend gave no detail", async () => {
    const { createToast } = await submitRejectedAnswer({ message: "Internal Server Error", status: 500 });

    await waitFor(() =>
      expect(createToast).toHaveBeenCalledWith({
        description: "Internal Server Error",
        title: "hitl:response.error",
        type: "error",
      }),
    );
  });

  it("leaves a 403 to the MutationCache handler but still reloads the stale request", async () => {
    const { createToast, invalidateQueries } = await submitRejectedAnswer({
      body: { detail: "Forbidden" },
      message: "Forbidden",
      status: 403,
    });

    await waitFor(() => expect(invalidateQueries).toHaveBeenCalledTimes(3));
    expect(createToast).not.toHaveBeenCalled();
  });
});
