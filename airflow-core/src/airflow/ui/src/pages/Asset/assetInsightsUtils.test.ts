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
import { describe, expect, it } from "vitest";

import { getAssetImpactSummary, getAssetType } from "./assetInsightsUtils";

describe("assetInsightsUtils", () => {
  it("classifies known asset URI patterns into display types", () => {
    expect(
      getAssetType({
        extra: null,
        group: "warehouse",
        name: "daily_orders",
        uri: "bigquery://proj.ds.daily_orders",
      }),
    ).toBe("BigQuery table");
    expect(
      getAssetType({
        extra: null,
        group: "analytics",
        name: "sales_dashboard",
        uri: "dashboard://superset/sales",
      }),
    ).toBe("Dashboard");
    expect(
      getAssetType({ extra: null, group: "landing", name: "orders.csv", uri: "file://incoming/orders.csv" }),
    ).toBe("CSV file");
  });

  it("extracts potentially impacted downstream dags, tasks, reports, and dashboards", () => {
    const summary = getAssetImpactSummary(1, {
      edges: [
        { source_id: "asset:1", target_id: "task:transform_dag:transform" },
        { source_id: "task:transform_dag:transform", target_id: "asset:2" },
        { source_id: "dag:transform_dag", target_id: "task:transform_dag:transform" },
        { source_id: "asset:2", target_id: "task:publish_dag:publish" },
        { source_id: "task:publish_dag:publish", target_id: "asset:3" },
        { source_id: "dag:publish_dag", target_id: "task:publish_dag:publish" },
        { source_id: "task:publish_dag:publish", target_id: "asset:4" },
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
        {
          id: "asset:4",
          name: "exec_dashboard",
          node_type: "asset",
          uri: "dashboard://superset/exec_dashboard",
        },
      ],
    });

    expect(summary.tasks.map((task) => task.label)).toEqual(["transform", "publish"]);
    expect(summary.dags.map((dag) => dag.label)).toEqual(["transform_dag", "publish_dag"]);
    expect(summary.reports.map((report) => report.label)).toEqual(["weekly_report"]);
    expect(summary.dashboards.map((dashboard) => dashboard.label)).toEqual(["exec_dashboard"]);
  });
});
