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
import type { ExternalViewResponse, PluginAppliesToResponse } from "openapi/requests/types.gen";

import { usePluginAppliesToContext } from "src/hooks/usePluginAppliesToContext";

import { usePluginTabs } from "./usePluginTabs";

vi.mock("openapi/queries", () => ({
  usePluginServiceGetPlugins: vi.fn(),
}));

vi.mock("src/context/colorMode", () => ({
  useColorMode: () => ({ colorMode: "light" }),
}));

vi.mock("src/hooks/usePluginAppliesToContext", () => ({
  usePluginAppliesToContext: vi.fn(),
}));

const mockUsePlugins = usePluginServiceGetPlugins as unknown as ReturnType<typeof vi.fn>;
const mockUseContext = usePluginAppliesToContext as unknown as ReturnType<typeof vi.fn>;

const makeView = (overrides: Partial<ExternalViewResponse>): ExternalViewResponse => ({
  destination: "dag_run",
  href: "/plugin/example",
  name: "Example",
  url_route: "example",
  ...overrides,
});

const setPlugins = (views: Array<ExternalViewResponse>) => {
  mockUsePlugins.mockReturnValue({
    data: { plugins: [{ external_views: views, react_apps: [] }] },
  });
};

const dag = {
  dag_id: "etl_sales",
  tags: [{ dag_display_name: "etl_sales", dag_id: "etl_sales", name: "ml" }],
};

describe("usePluginTabs", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseContext.mockReturnValue({ dag, isLoading: false });
  });

  it.each([
    ["a matching applies_to", { dag_tags: ["ml"] }, 1],
    ["a non-matching applies_to", { dag_ids: ["etl_orders"] }, 0],
  ])("includes the right tabs for %s", (_label, appliesTo: PluginAppliesToResponse, expected) => {
    setPlugins([makeView({ applies_to: appliesTo })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(expected);
  });

  it("includes a tab with no applies_to on every Dag", () => {
    setPlugins([makeView({})]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(1);
    expect(result.current[0]?.value).toBe("plugin/example");
  });

  it("excludes views for a different destination", () => {
    setPlugins([makeView({ destination: "task_instance" })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(0);
  });

  it("withholds a scoped tab until its context resolves, avoiding a flicker", () => {
    mockUseContext.mockReturnValue({ dag: undefined, isLoading: true });
    setPlugins([makeView({ applies_to: { dag_tags: ["ml"] } }), makeView({ url_route: "always" })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(1);
    expect(result.current[0]?.value).toBe("plugin/always");
  });

  it.each([
    ["no view is scoped", [{}], false],
    [
      "a view for another destination is scoped",
      [{ applies_to: { dag_ids: ["x"] }, destination: "task" as const }],
      false,
    ],
    ["a view for this destination is scoped", [{ applies_to: { dag_ids: ["x"] } }], true],
  ])("resolves the context only when %s", (_label, views: Array<Partial<ExternalViewResponse>>, enabled) => {
    setPlugins(views.map(makeView));

    renderHook(() => usePluginTabs("dag_run"));

    expect(mockUseContext).toHaveBeenCalledWith(enabled);
  });
});
