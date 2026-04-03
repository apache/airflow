/* eslint-disable unicorn/no-null */

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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { HITLDetailHistory, TaskInstanceHistoryResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { HITLResponseForm } from "./HITLResponseForm";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("src/queries/useUpdateHITLDetail", () => ({
  useUpdateHITLDetail: () => ({ updateHITLResponse: vi.fn() }),
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
