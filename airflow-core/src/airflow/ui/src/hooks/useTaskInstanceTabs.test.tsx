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
import { renderHook } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { TaskInstanceTab } from "src/constants/tab";

import { useTaskInstanceTabs } from "./useTaskInstanceTabs";

const dagId = "etl_sales";
const runId = "manual__2026-01-01";
const taskId = "train_model";

const versionNumber = 3;

const params = { dagId, isVersionKnown: true, taskId, versionNumber };

const TABS = [
  { icon: undefined, label: "Logs", value: TaskInstanceTab.Logs },
  { icon: undefined, label: "Rendered Templates", value: TaskInstanceTab.RenderedTemplates },
  { icon: undefined, label: "Asset Events", value: TaskInstanceTab.AssetEvents },
];

let mockPathname = `/dags/${dagId}/runs/${runId}/tasks/${taskId}/logs`;

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useLocation: () => ({ pathname: mockPathname }),
}));

const mocks = vi.hoisted(() => ({
  getTask: vi.fn(),
  task: undefined as { has_outlets: boolean; template_fields: Array<string> } | undefined,
}));

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useTaskServiceGetTask: (...args: Array<unknown>) => {
    mocks.getTask(...args);

    return { data: mocks.task };
  },
}));

const tabValues = () =>
  renderHook(() => useTaskInstanceTabs(params, TABS)).result.current.tabs.map((tab) => tab.value);

describe("useTaskInstanceTabs", () => {
  beforeEach(() => {
    mockPathname = `/dags/${dagId}/runs/${runId}/tasks/${taskId}/logs`;
    mocks.task = { has_outlets: true, template_fields: ["bash_command"] };
    mocks.getTask.mockClear();
  });

  it("pins the lookup to the version the instance ran", () => {
    tabValues();

    expect(mocks.getTask).toHaveBeenCalledWith(
      { dagId, taskId, versionNumber },
      undefined,
      expect.objectContaining({ enabled: true }),
    );
  });

  it("does not fetch before the version is known, so the answer cannot change under the user", () => {
    renderHook(() => useTaskInstanceTabs({ ...params, isVersionKnown: false }, TABS));

    expect(mocks.getTask).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });

  it("keeps every tab the definition allows", () => {
    expect(tabValues()).toEqual(TABS.map((tab) => tab.value));
  });

  it("keeps every tab while the definition is still unknown", () => {
    mocks.task = undefined;

    expect(tabValues()).toHaveLength(TABS.length);
  });

  it("drops asset events when the task declares no outlets", () => {
    mocks.task = { has_outlets: false, template_fields: ["bash_command"] };

    expect(tabValues()).not.toContain(TaskInstanceTab.AssetEvents);
  });

  it("drops rendered templates when the task has no template fields", () => {
    mocks.task = { has_outlets: true, template_fields: [] };

    expect(tabValues()).not.toContain(TaskInstanceTab.RenderedTemplates);
  });

  it("leaves tabs it has no rule for alone", () => {
    mocks.task = { has_outlets: false, template_fields: [] };

    expect(tabValues()).toEqual([TaskInstanceTab.Logs]);
  });

  it("keeps a hidden tab the user is already on, so deep links still work", () => {
    mocks.task = { has_outlets: false, template_fields: ["bash_command"] };
    mockPathname = `/dags/${dagId}/runs/${runId}/tasks/${taskId}/${TaskInstanceTab.AssetEvents}`;

    expect(tabValues()).toContain(TaskInstanceTab.AssetEvents);
  });
});
