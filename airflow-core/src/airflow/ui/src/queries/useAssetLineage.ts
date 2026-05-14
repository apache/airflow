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
import { useQuery } from "@tanstack/react-query";

import type { AssetLineageGraphResponse } from "openapi/requests/types.gen";
import { AssetService } from "openapi/requests/services.gen";

type LineageMode = "asset_only" | "full";

const mockEnabled = () => new URLSearchParams(globalThis.location.search).get("mockAssets") === "true";
const mockErrorEnabled = () =>
  new URLSearchParams(globalThis.location.search).get("mockAssetsError") === "true";

const mockLineageData: Record<LineageMode, AssetLineageGraphResponse> = {
  asset_only: {
    edges: [
      {
        column_lineage: {
          player_name: [
            {
              source_asset_uri: "file://incoming/player-stats/team_b_raw.csv",
              source_column: "player_name",
            },
          ],
        },
        source_id: "asset:3",
        target_id: "asset:1",
      },
      {
        column_lineage: {
          odds: [
            {
              source_asset_uri: "file://incoming/player-stats/team_b.csv",
              source_column: "average_odds",
            },
          ],
        },
        source_id: "asset:1",
        target_id: "asset:2",
      },
    ],
    nodes: [
      { group: "demo", id: "asset:3", name: "raw_player_data", node_type: "asset", uri: "file://incoming/player-stats/team_b_raw.csv" },
      { group: "demo", id: "asset:1", name: "team_b_player_stats", node_type: "asset", uri: "file://incoming/player-stats/team_b.csv" },
      { group: "demo", id: "asset:2", name: "compute_player_odds", node_type: "asset", uri: "file://incoming/player-stats/odds.csv" },
    ],
  },
  full: {
    edges: [
      { source_id: "dag:upstream_dag", target_id: "task:upstream_dag:producer_task" },
      { source_id: "task:upstream_dag:producer_task", target_id: "asset:1" },
      { source_id: "asset:1", target_id: "task:downstream_dag:consumer_task" },
      { source_id: "task:downstream_dag:consumer_task", target_id: "asset:2" },
    ],
    nodes: [
      { id: "dag:upstream_dag", name: "upstream_dag", node_type: "dag" },
      { id: "task:upstream_dag:producer_task", name: "producer_task", node_type: "task" },
      { group: "demo", id: "asset:1", name: "team_b_player_stats", node_type: "asset", uri: "file://incoming/player-stats/team_b.csv" },
      { id: "task:downstream_dag:consumer_task", name: "consumer_task", node_type: "task" },
      { group: "demo", id: "asset:2", name: "compute_player_odds", node_type: "asset", uri: "file://incoming/player-stats/odds.csv" },
    ],
  },
};

export const useAssetLineage = (
  assetId: string | undefined,
  options?: { depth?: number; mode?: LineageMode },
) => {
  const useMock = mockEnabled();
  const mode = options?.mode ?? "full";
  const parsedAssetId = assetId === undefined ? 0 : parseInt(assetId, 10);

  return useQuery({
    enabled: Boolean(assetId),
    queryFn: async () => {
      if (useMock) {
        if (mockErrorEnabled()) {
          throw new Error("Mock asset lineage request failed.");
        }

        return mockLineageData[mode];
      }

      return mode === "asset_only"
        ? AssetService.getAssetOnlyLineage({ assetId: parsedAssetId, depth: options?.depth })
        : AssetService.getAssetLineage({ assetId: parsedAssetId, depth: options?.depth });
    },
    queryKey: ["asset-lineage", parsedAssetId, options?.depth, mode, useMock],
  });
};
