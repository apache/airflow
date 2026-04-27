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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { AssetInsightsPanel } from "./AssetInsightsPanel";

const { mockUseAssetLineage } = vi.hoisted(() => ({
  mockUseAssetLineage: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (translationKey: string, options?: { defaultValue?: string }) =>
      options?.defaultValue ?? translationKey,
  }),
}));

vi.mock("src/queries/useAssetLineage", () => ({
  useAssetLineage: mockUseAssetLineage,
}));

describe("AssetInsightsPanel", () => {
  beforeEach(() => {
    mockUseAssetLineage.mockReset();
    mockUseAssetLineage.mockReturnValue({
      data: {
        edges: [
          { source_id: "asset:1", target_id: "task:transform_dag:transform" },
          { source_id: "task:transform_dag:transform", target_id: "asset:2" },
          { source_id: "dag:transform_dag", target_id: "task:transform_dag:transform" },
          { source_id: "asset:2", target_id: "task:publish_dag:publish" },
          { source_id: "task:publish_dag:publish", target_id: "asset:3" },
          { source_id: "dag:publish_dag", target_id: "task:publish_dag:publish" },
        ],
        nodes: [
          { id: "asset:1", name: "raw_orders", node_type: "asset", uri: "s3://bucket/raw_orders" },
          { id: "task:transform_dag:transform", name: "transform", node_type: "task" },
          { id: "dag:transform_dag", name: "transform_dag", node_type: "dag" },
          {
            id: "asset:2",
            name: "analytics.player_stats",
            node_type: "asset",
            uri: "snowflake://db/schema/player_stats",
          },
          { id: "task:publish_dag:publish", name: "publish", node_type: "task" },
          { id: "dag:publish_dag", name: "publish_dag", node_type: "dag" },
          { id: "asset:3", name: "weekly_report", node_type: "asset", uri: "report://finance/weekly_report" },
        ],
      },
    });
  });

  it("requests lineage data with depth 10 for impact analysis", () => {
    render(
      <AssetInsightsPanel
        asset={{
          aliases: [],
          consuming_tasks: [],
          created_at: "",
          extra: null,
          group: "warehouse",
          id: 1,
          last_asset_event: null,
          name: "raw_orders",
          producing_tasks: [],
          scheduled_dags: [],
          updated_at: "",
          uri: "s3://bucket/raw_orders",
          watchers: [],
        }}
      />,
      { wrapper: Wrapper },
    );

    expect(mockUseAssetLineage).toHaveBeenCalledWith("1", { depth: 10 });
  });

  it("renders impacted downstream dags, tasks, and reports from lineage data", () => {
    render(
      <AssetInsightsPanel
        asset={{
          aliases: [],
          consuming_tasks: [{ created_at: "", dag_id: "publish_dag", task_id: "publish", updated_at: "" }],
          created_at: "",
          extra: null,
          group: "warehouse",
          id: 1,
          last_asset_event: null,
          name: "raw_orders",
          producing_tasks: [{ created_at: "", dag_id: "transform_dag", task_id: "extract", updated_at: "" }],
          scheduled_dags: [{ created_at: "", dag_id: "publish_dag", updated_at: "" }],
          updated_at: "",
          uri: "s3://bucket/raw_orders",
          watchers: [],
        }}
      />,
      { wrapper: Wrapper },
    );

    const dagStat = screen.getByText("Potential DAGs").closest('[data-testid="stat"]');

    expect(dagStat).not.toBeNull();
    expect(within(dagStat as HTMLElement).getByText("2")).toBeInTheDocument();
    const downstreamDagsSection = screen.getByText("Downstream DAGs").parentElement;
    const downstreamTasksSection = screen.getByText("Downstream Tasks").parentElement;
    const downstreamReportsSection = screen.getByText("Downstream Reports").parentElement;

    expect(downstreamDagsSection).not.toBeNull();
    expect(downstreamTasksSection).not.toBeNull();
    expect(downstreamReportsSection).not.toBeNull();

    expect(within(downstreamDagsSection as HTMLElement).getByRole("link", { name: "transform_dag" })).toHaveAttribute(
      "href",
      "/dags/transform_dag",
    );
    expect(within(downstreamDagsSection as HTMLElement).getByRole("link", { name: "publish_dag" })).toHaveAttribute(
      "href",
      "/dags/publish_dag",
    );
    expect(within(downstreamTasksSection as HTMLElement).getByRole("link", { name: "transform" })).toHaveAttribute(
      "href",
      "/dags/transform_dag/tasks/transform",
    );
    expect(within(downstreamTasksSection as HTMLElement).getByRole("link", { name: "publish" })).toHaveAttribute(
      "href",
      "/dags/publish_dag/tasks/publish",
    );
    expect(
      within(downstreamReportsSection as HTMLElement).getByRole("link", { name: "weekly_report" }),
    ).toHaveAttribute("href", "/assets/3");
  });
});
