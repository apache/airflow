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
import { beforeEach, describe, expect, it, vi } from "vitest";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { PluginLinkActionResponse } from "openapi/requests/types.gen";

import { usePluginAppliesToContext } from "src/hooks/usePluginAppliesToContext";

import { usePluginActions } from "./usePluginActions";

vi.mock("openapi/queries", () => ({
  usePluginServiceGetPlugins: vi.fn(),
}));

vi.mock("src/hooks/usePluginAppliesToContext", () => ({
  usePluginAppliesToContext: vi.fn(),
}));

const mockUsePlugins = usePluginServiceGetPlugins as unknown as ReturnType<typeof vi.fn>;
const mockUseContext = usePluginAppliesToContext as unknown as ReturnType<typeof vi.fn>;

const makeAction = (overrides: Partial<PluginLinkActionResponse> = {}): PluginLinkActionResponse => ({
  action_type: "link",
  destination: "dag_run",
  href: "https://example.com/{DAG_ID}",
  name: "Runbook",
  ...overrides,
});

const setPlugins = (plugins: Array<Record<string, unknown>>) => {
  mockUsePlugins.mockReturnValue({ data: { plugins } });
};

describe("usePluginActions", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseContext.mockReturnValue({ isLoading: false });
  });

  it("returns nothing when no plugins are loaded", () => {
    mockUsePlugins.mockReturnValue({ data: undefined });

    const { result } = renderHook(() => usePluginActions("dag_run"));

    expect(result.current.actions).toEqual([]);
  });

  it("tolerates a plugin that predates the field", () => {
    setPlugins([{ external_views: [], react_apps: [] }]);

    const { result } = renderHook(() => usePluginActions("dag_run"));

    expect(result.current.actions).toEqual([]);
  });

  it("collects actions across plugins for the requested destination only", () => {
    const wanted = makeAction({ name: "Wanted" });

    setPlugins([
      { ui_actions: [wanted] },
      { ui_actions: [makeAction({ destination: "dag", name: "Other destination" })] },
    ]);

    const { result } = renderHook(() => usePluginActions("dag_run"));

    expect(result.current.actions).toEqual([wanted]);
  });

  it("ignores external views and React apps, which are not actions", () => {
    setPlugins([
      {
        external_views: [{ destination: "dag_run", href: "/x", name: "A view", url_route: "v" }],
        react_apps: [{ bundle_url: "/b.js", destination: "dag_run", name: "An app", url_route: "a" }],
        ui_actions: [],
      },
    ]);

    const { result } = renderHook(() => usePluginActions("dag_run"));

    expect(result.current.actions).toEqual([]);
  });

  // The route context mounts three query observers, so it must stay switched off whenever
  // nothing actually needs it.
  it("leaves the route context disabled when no matching action is scoped", () => {
    setPlugins([
      { ui_actions: [makeAction()] },
      { ui_actions: [makeAction({ applies_to: { "dag.tags.name": ["ml"] }, destination: "dag" })] },
    ]);

    renderHook(() => usePluginActions("dag_run"));

    expect(mockUseContext).toHaveBeenCalledWith(false);
  });

  it("enables the route context when a matching action is scoped", () => {
    setPlugins([{ ui_actions: [makeAction({ applies_to: { "dag.tags.name": ["ml"] } })] }]);

    renderHook(() => usePluginActions("dag_run"));

    expect(mockUseContext).toHaveBeenCalledWith(true);
  });

  it("treats an applies_to with only empty lists as unscoped", () => {
    setPlugins([{ ui_actions: [makeAction({ applies_to: { "dag.dag_id": [] } })] }]);

    renderHook(() => usePluginActions("dag_run"));

    expect(mockUseContext).toHaveBeenCalledWith(false);
  });

  it("keeps the actions array identity stable across re-renders", () => {
    setPlugins([{ ui_actions: [makeAction()] }]);

    const { rerender, result } = renderHook(() => usePluginActions("dag_run"));
    const first = result.current.actions;

    rerender();

    // The column factories take this by reference; a new array each render would rebuild
    // every column on every render.
    expect(result.current.actions).toBe(first);
  });
});
