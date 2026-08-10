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
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

const RUN_ON_DEMAND_SECTION_LABEL = "dags:runAndTaskActions.onDemandSection.button";
const ON_DEMAND_SECTION_READY_TITLE = "dags:runAndTaskActions.onDemandSection.readyTitle";

vi.mock("src/components/Clear", () => ({
  ClearTaskInstanceButton: () => <button type="button">clear</button>,
}));

vi.mock("src/components/Clear/TaskInstance/ClearTaskInstanceDialog", () => ({
  default: () => undefined,
}));

vi.mock("src/components/MarkAs", () => ({
  MarkTaskInstanceAsButton: () => <button type="button">mark</button>,
}));

vi.mock("src/components/HeaderCard", () => ({
  HeaderCard: ({ actions, title }: { readonly actions: ReactNode; readonly title: ReactNode }) => (
    <div>
      <h1>{title}</h1>
      <div>{actions}</div>
    </div>
  ),
}));

vi.mock("src/components/NotePreview", () => ({
  default: () => undefined,
}));

const taskInstance = {
  dag_id: "example",
  dag_run_id: "manual__2026-07-23T23:01:47",
  dag_version: {
    id: "01983274-c8e0-7ff1-b235-a162fc5e034f",
    version_number: 1,
  },
  duration: 1,
  end_date: "2026-07-23T23:01:48.000Z",
  map_index: -1,
  note: null,
  operator: "OnDemandSectionOperator",
  operator_name: "OnDemandSectionOperator",
  rendered_map_index: null,
  start_date: "2026-07-23T23:01:47.000Z",
  state: "success",
  task_display_name: "on_demand_section",
  task_id: "on_demand_section",
  try_number: 1,
} as TaskInstanceResponse;

const renderHeader = (overrides: Partial<TaskInstanceResponse> = {}) =>
  render(<Header taskInstance={{ ...taskInstance, ...overrides }} />, { wrapper: Wrapper });

describe("TaskInstance Header on-demand section callout", () => {
  it("shows an on-demand section callout for runnable on-demand sections", () => {
    renderHeader();

    expect(screen.getByText(ON_DEMAND_SECTION_READY_TITLE)).not.toBeNull();
    expect(screen.getAllByRole("button", { name: RUN_ON_DEMAND_SECTION_LABEL })).toHaveLength(1);
  });

  it("does not show an on-demand section callout for other tasks", () => {
    renderHeader({
      operator: "BashOperator",
      operator_name: "BashOperator",
    });

    expect(screen.queryByText(ON_DEMAND_SECTION_READY_TITLE)).toBeNull();
    expect(screen.queryByRole("button", { name: RUN_ON_DEMAND_SECTION_LABEL })).toBeNull();
  });

  it("does not show an on-demand section callout for unfinished on-demand sections", () => {
    renderHeader({ state: "running" });

    expect(screen.queryByText(ON_DEMAND_SECTION_READY_TITLE)).toBeNull();
    expect(screen.queryByRole("button", { name: RUN_ON_DEMAND_SECTION_LABEL })).toBeNull();
  });
});
