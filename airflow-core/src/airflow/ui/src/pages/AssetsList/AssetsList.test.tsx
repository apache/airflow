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
import { render, screen } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { FilterConfig } from "src/components/FilterBar";
import { Wrapper } from "src/utils/Wrapper";

import { AssetsList } from "./AssetsList";

let mockSearchParams = new URLSearchParams();

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useSearchParams: () => [mockSearchParams, vi.fn()] as const,
  };
});

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useAssetServiceGetAssetsUi: vi.fn(),
  };
});

vi.mock("src/components/DataTable", () => ({
  DataTable: () => null,
}));

vi.mock("src/components/FilterBar", () => ({
  FilterBar: ({ configs }: { readonly configs: Array<FilterConfig> }) => (
    <div data-testid="asset-filters">
      {configs.map(({ key, supportsAdvancedSearch }) => (
        <span
          data-advanced-search={supportsAdvancedSearch === true}
          data-testid={`asset-filter-${key}`}
          key={key}
        >
          {key}
        </span>
      ))}
    </div>
  ),
}));

vi.mock("src/components/SearchBar", () => ({
  SearchBar: () => null,
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 50 : false),
}));

const { useAssetServiceGetAssetsUi } = await import("openapi/queries");

const lastAssetsCall = () => vi.mocked(useAssetServiceGetAssetsUi).mock.calls.at(-1)?.[0];

describe("AssetsList filters", () => {
  beforeEach(() => {
    mockSearchParams = new URLSearchParams();
    vi.mocked(useAssetServiceGetAssetsUi).mockReturnValue({
      data: { assets: [], total_entries: 0 },
      error: null,
      isLoading: false,
    } as ReturnType<typeof useAssetServiceGetAssetsUi>);
  });

  it("offers an exact-match Dag ID filter", () => {
    render(<AssetsList />, { wrapper: Wrapper });

    expect(screen.getByTestId("asset-filter-dag_id")).toHaveAttribute("data-advanced-search", "false");
  });

  it("passes the selected Dag ID to the Assets API and omits it after clearing", () => {
    mockSearchParams = new URLSearchParams("dag_id=consumer_dag");

    const { rerender } = render(<AssetsList />, { wrapper: Wrapper });

    expect(lastAssetsCall()?.dagIds).toEqual(["consumer_dag"]);

    mockSearchParams = new URLSearchParams("dag_id=");
    rerender(<AssetsList />);

    expect(lastAssetsCall()?.dagIds).toBeUndefined();
  });
});
