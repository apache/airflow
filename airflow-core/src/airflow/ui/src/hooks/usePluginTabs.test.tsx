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

import { useDagServiceGetDag, usePluginServiceGetPlugins } from "openapi/queries";
import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { usePluginTabs } from "./usePluginTabs";

vi.mock("openapi/queries", () => ({
  useDagServiceGetDag: vi.fn(),
  usePluginServiceGetPlugins: vi.fn(),
}));

vi.mock("react-router-dom", () => ({
  useParams: () => ({ dagId: "etl_sales" }),
}));

vi.mock("src/context/colorMode", () => ({
  useColorMode: () => ({ colorMode: "light" }),
}));

const mockUseDag = useDagServiceGetDag as unknown as ReturnType<typeof vi.fn>;
const mockUsePlugins = usePluginServiceGetPlugins as unknown as ReturnType<typeof vi.fn>;

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

const setDag = (tagNames: Array<string>) => {
  mockUseDag.mockReturnValue({
    data: {
      dag_id: "etl_sales",
      tags: tagNames.map((name) => ({ dag_display_name: "etl_sales", dag_id: "etl_sales", name })),
    },
  });
};

describe("usePluginTabs", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setDag(["ml"]);
  });

  it("includes a tab whose filter matches the current Dag", () => {
    setPlugins([makeView({ dag_tags: ["ml"] })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(1);
    expect(result.current[0]?.value).toBe("plugin/example");
  });

  it("excludes a tab whose filter does not match the current Dag", () => {
    setPlugins([makeView({ dag_ids: ["etl_orders"] })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(0);
  });

  it("includes a tab with no Dag filter on every Dag", () => {
    setPlugins([makeView({})]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(1);
  });

  it("excludes views for a different destination", () => {
    setPlugins([makeView({ destination: "task_instance" })]);

    const { result } = renderHook(() => usePluginTabs("dag_run"));

    expect(result.current).toHaveLength(0);
  });
});
