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
import { fireEvent, render, screen } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { AssetLayout } from "./AssetLayout";

const {
  mockUseAssetLineage,
  mockUseAssetDetailData,
  mockUseAssetEventsData,
  mockSetTableURLState,
  mockFitView,
  mockGetZoom,
} = vi.hoisted(() => ({
  mockUseAssetLineage: vi.fn(),
  mockUseAssetDetailData: vi.fn(),
  mockUseAssetEventsData: vi.fn(),
  mockSetTableURLState: vi.fn(),
  mockFitView: vi.fn(),
  mockGetZoom: vi.fn(() => 1),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr" },
    t: (translationKey: string, options?: { defaultValue?: string }) =>
      options?.defaultValue ?? translationKey,
  }),
}));

vi.mock("@xyflow/react", () => ({
  useReactFlow: () => ({
    fitView: mockFitView,
    getZoom: mockGetZoom,
  }),
}));

vi.mock("src/queries/useAssetLineage", () => ({
  useAssetLineage: mockUseAssetLineage,
}));

vi.mock("src/queries/useMockAssetData", () => ({
  useAssetDetailData: mockUseAssetDetailData,
  useAssetEventsData: mockUseAssetEventsData,
}));

vi.mock("src/components/DataTable/useTableUrlState", () => ({
  useTableURLState: () => ({
    setTableURLState: mockSetTableURLState,
    tableURLState: {
      pagination: { pageIndex: 0, pageSize: 25 },
      sorting: [],
    },
  }),
}));

vi.mock("src/components/BreadcrumbStats", () => ({
  BreadcrumbStats: () => <div data-testid="breadcrumbs" />,
}));

vi.mock("src/components/SearchBar", () => ({
  SearchBar: ({ onChange, placeholder }: { readonly onChange: (value: string) => void; readonly placeholder: string }) => (
    <input
      aria-label={placeholder}
      onChange={(event) => onChange(event.currentTarget.value)}
      placeholder={placeholder}
    />
  ),
}));

vi.mock("src/context/openGroups", () => ({
  OpenGroupsProvider: ({ children }: PropsWithChildren) => <>{children}</>,
}));

vi.mock("./AssetGraph", () => ({
  AssetGraph: () => <div data-testid="asset-graph" />,
}));

vi.mock("./AssetInsightsPanel", () => ({
  AssetInsightsPanel: () => <div data-testid="asset-insights-panel">Insights</div>,
}));

vi.mock("./AssetLineageGraph", () => ({
  AssetLineageGraph: ({
    activeNodeId,
    lineageData,
    setActiveNodeId,
  }: {
    readonly activeNodeId?: string;
    readonly lineageData: { edges: Array<unknown>; nodes: Array<unknown> };
    readonly setActiveNodeId: (value: string) => void;
  }) => (
    <div data-testid="asset-lineage-graph">
      <div>{`active:${activeNodeId ?? "none"}`}</div>
      <div>{`nodes:${lineageData.nodes.length}`}</div>
      <button onClick={() => setActiveNodeId("asset:34")} type="button">
        Select metrics asset
      </button>
      <button onClick={() => setActiveNodeId("asset:33")} type="button">
        Select curated asset
      </button>
    </div>
  ),
}));

vi.mock("./AssetPanelButtons", () => ({
  AssetPanelButtons: ({
    dependencyType,
    setDependencyType,
  }: {
    readonly dependencyType: string;
    readonly setDependencyType: (value: "data" | "lineage" | "scheduling") => void;
  }) => (
    <div>
      <div>{`dependency:${dependencyType}`}</div>
      <button onClick={() => setDependencyType("lineage")} type="button">
        Switch to Lineage
      </button>
      <button onClick={() => setDependencyType("data")} type="button">
        Switch to Data
      </button>
    </div>
  ),
}));

vi.mock("./CreateAssetEvent", () => ({
  CreateAssetEvent: () => <div data-testid="create-asset-event" />,
}));

vi.mock("./Header", () => ({
  Header: ({ asset }: { readonly asset?: { name?: string } }) => <div>{asset?.name ?? "No asset"}</div>,
}));

vi.mock("src/components/Assets/AssetEvents", () => ({
  AssetEvents: () => <div data-testid="asset-events">Asset Events</div>,
}));

const assetOnlyLineageData = {
  edges: [
    {
      column_lineage: {
        customer_id: [
          {
            source_asset_uri: "warehouse://raw/orders",
            source_column: "customer_id",
          },
        ],
        order_id: [
          {
            source_asset_uri: "warehouse://raw/orders",
            source_column: "id",
          },
        ],
      },
      source_id: "asset:32",
      target_id: "asset:33",
    },
    {
      column_lineage: {
        gross_revenue: [
          {
            source_asset_uri: "warehouse://curated/orders",
            source_column: "total_amount",
          },
        ],
      },
      source_id: "asset:33",
      target_id: "asset:34",
    },
  ],
  nodes: [
    { group: "asset", id: "asset:32", name: "raw_orders", node_type: "asset", uri: "warehouse://raw/orders" },
    { group: "asset", id: "asset:33", name: "curated_orders", node_type: "asset", uri: "warehouse://curated/orders" },
    { group: "asset", id: "asset:34", name: "customer_order_metrics", node_type: "asset", uri: "warehouse://metrics/customer_orders" },
  ],
};

const fullLineageData = {
  edges: [{ source_id: "dag:lineage", target_id: "task:lineage:build" }],
  nodes: [{ id: "dag:lineage", name: "lineage", node_type: "dag" }],
};

const renderAssetLayout = () =>
  render(
    <MemoryRouter initialEntries={["/assets/33?mockAssets=true"]}>
      <Routes>
        <Route element={<AssetLayout />} path="/assets/:assetId" />
      </Routes>
    </MemoryRouter>,
    { wrapper: BaseWrapper },
  );

describe("AssetLayout", () => {
  beforeEach(() => {
    mockUseAssetLineage.mockReset();
    mockUseAssetDetailData.mockReset();
    mockUseAssetEventsData.mockReset();
    mockSetTableURLState.mockReset();

    mockUseAssetDetailData.mockReturnValue({
      data: {
        aliases: [],
        consuming_tasks: [],
        created_at: "",
        extra: { owner: "demo" },
        group: "asset",
        id: 33,
        last_asset_event: null,
        name: "curated_orders",
        producing_tasks: [],
        scheduled_dags: [],
        updated_at: "",
        uri: "warehouse://curated/orders",
        watchers: [],
      },
      isLoading: false,
    });

    mockUseAssetEventsData.mockReturnValue({
      data: { asset_events: [], total_entries: 0 },
      isLoading: false,
    });

    mockUseAssetLineage.mockImplementation((_assetId: string, options?: { mode?: "asset_only" | "full" }) => ({
      data: options?.mode === "asset_only" ? assetOnlyLineageData : fullLineageData,
      error: undefined,
      isError: false,
      isLoading: false,
    }));
  });

  it("requests full lineage by default and switches to asset-only mode", () => {
    renderAssetLayout();

    expect(mockUseAssetLineage).toHaveBeenCalledWith("33", { mode: "full" });

    fireEvent.click(screen.getByRole("button", { name: "Switch to Lineage" }));
    fireEvent.click(screen.getByRole("button", { name: "Asset Only" }));

    expect(mockUseAssetLineage).toHaveBeenLastCalledWith("33", { mode: "asset_only" });
  });

  it("shows column lineage for the selected asset in asset-only mode", () => {
    renderAssetLayout();

    fireEvent.click(screen.getByRole("button", { name: "Switch to Lineage" }));
    fireEvent.click(screen.getByRole("button", { name: "Asset Only" }));

    expect(screen.getByText("Column Lineage")).toBeInTheDocument();
    expect(screen.getByText("raw_orders -> curated_orders")).toBeInTheDocument();
    expect(screen.getByText("curated_orders.order_id <- raw_orders.id")).toBeInTheDocument();
    expect(screen.getByText("curated_orders.customer_id <- raw_orders.customer_id")).toBeInTheDocument();
  });

  it("updates column lineage when a different asset node is selected", () => {
    renderAssetLayout();

    fireEvent.click(screen.getByRole("button", { name: "Switch to Lineage" }));
    fireEvent.click(screen.getByRole("button", { name: "Asset Only" }));
    fireEvent.click(screen.getByRole("button", { name: "Select metrics asset" }));

    expect(screen.getByText("curated_orders -> customer_order_metrics")).toBeInTheDocument();
    expect(
      screen.getByText("customer_order_metrics.gross_revenue <- curated_orders.total_amount"),
    ).toBeInTheDocument();
  });

  it("renders the non-lineage view when data mode is selected", () => {
    renderAssetLayout();

    fireEvent.click(screen.getByRole("button", { name: "Switch to Data" }));

    expect(screen.getByTestId("asset-graph")).toBeInTheDocument();
    expect(screen.queryByTestId("asset-lineage-graph")).not.toBeInTheDocument();
  });
});
