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

import {
  useAssetServiceGetAsset,
  useAssetServiceGetAssets,
  useAssetServiceGetAssetEvents,
} from "openapi/queries";
import type {
  AssetCollectionResponse,
  AssetEventCollectionResponse,
  AssetResponse,
} from "openapi/requests/types.gen";

const now = "2026-03-11T14:00:00Z";

const mockAssets: Array<AssetResponse> = [
  {
    aliases: [],
    consuming_tasks: [
      {
        created_at: now,
        dag_id: "downstream_dag",
        task_id: "consumer_task",
        updated_at: now,
      },
    ],
    created_at: now,
    extra: { owner: "demo", team: "frontend" },
    group: "demo",
    id: 1,
    last_asset_event: { id: 101, timestamp: now },
    name: "target_asset",
    producing_tasks: [
      {
        created_at: now,
        dag_id: "upstream_dag",
        task_id: "producer_task",
        updated_at: now,
      },
    ],
    scheduled_dags: [{ created_at: now, dag_id: "downstream_dag", updated_at: now }],
    updated_at: now,
    uri: "s3://demo/target_asset",
    watchers: [],
  },
  {
    aliases: [],
    consuming_tasks: [],
    created_at: now,
    extra: { owner: "demo" },
    group: "demo",
    id: 2,
    last_asset_event: { id: 102, timestamp: "2026-03-10T10:00:00Z" },
    name: "target_asset_2",
    producing_tasks: [
      {
        created_at: now,
        dag_id: "downstream_dag",
        task_id: "consumer_task",
        updated_at: now,
      },
    ],
    scheduled_dags: [],
    updated_at: now,
    uri: "s3://demo/target_asset_2",
    watchers: [],
  },
];

const mockAssetEvents: AssetEventCollectionResponse = {
  asset_events: [
    {
      asset_id: 1,
      created_dagruns: [],
      extra: { from_rest_api: true, note: "mock event" },
      group: "demo",
      id: 101,
      name: "target_asset",
      partition_key: "2026-03-11",
      source_dag_id: "upstream_dag",
      source_map_index: -1,
      source_run_id: "manual__2026-03-11T14:00:00+00:00",
      source_task_id: "producer_task",
      timestamp: now,
      uri: "s3://demo/target_asset",
    },
    {
      asset_id: 1,
      created_dagruns: [],
      extra: { from_trigger: true },
      group: "demo",
      id: 100,
      name: "target_asset",
      partition_key: "2026-03-10",
      source_dag_id: "upstream_dag",
      source_map_index: -1,
      source_run_id: "manual__2026-03-10T10:00:00+00:00",
      source_task_id: "producer_task",
      timestamp: "2026-03-10T10:00:00Z",
      uri: "s3://demo/target_asset",
    },
  ],
  total_entries: 2,
};

const mockEnabled = () => new URLSearchParams(globalThis.location.search).get("mockAssets") === "true";
const mockErrorEnabled = () =>
  new URLSearchParams(globalThis.location.search).get("mockAssetsError") === "true";

const filterAssets = (assets: Array<AssetResponse>, namePattern?: string) => {
  if (namePattern === undefined || namePattern === "") {
    return assets;
  }

  const normalized = namePattern.toLowerCase();

  return assets.filter(
    (asset) => asset.name.toLowerCase().includes(normalized) || asset.uri.toLowerCase().includes(normalized),
  );
};

export const useAssetListData = ({
  limit,
  namePattern,
  offset,
  orderBy,
}: {
  limit?: number;
  namePattern?: string;
  offset?: number;
  orderBy?: Array<string>;
}) => {
  const useMock = mockEnabled();
  const realQuery = useAssetServiceGetAssets({ limit, namePattern, offset, orderBy }, undefined, {
    enabled: !useMock,
  });
  const mockQuery = useQuery<AssetCollectionResponse>({
    enabled: useMock,
    queryFn: async () =>
      new Promise<AssetCollectionResponse>((resolve, reject) => {
        setTimeout(() => {
          if (mockErrorEnabled()) {
            reject(new Error("Mock asset list request failed."));

            return;
          }

          const filteredAssets = filterAssets(mockAssets, namePattern);
          const start = offset ?? 0;
          const end = limit === undefined ? undefined : start + limit;

          resolve({
            assets: filteredAssets.slice(start, end),
            total_entries: filteredAssets.length,
          });
        }, 300);
      }),
    queryKey: ["mockAssetsList", limit, namePattern, offset, orderBy],
  });

  return useMock ? mockQuery : realQuery;
};

export const useAssetDetailData = (assetId?: number) => {
  const useMock = mockEnabled();
  const realQuery = useAssetServiceGetAsset({ assetId: assetId ?? 0 }, undefined, {
    enabled: !useMock && assetId !== undefined,
  });
  const mockQuery = useQuery<AssetResponse | undefined>({
    enabled: useMock && assetId !== undefined,
    queryFn: async () =>
      new Promise<AssetResponse | undefined>((resolve, reject) => {
        setTimeout(() => {
          if (mockErrorEnabled()) {
            reject(new Error("Mock asset detail request failed."));

            return;
          }

          resolve(mockAssets.find((asset) => asset.id === assetId));
        }, 300);
      }),
    queryKey: ["mockAssetDetail", assetId],
  });

  return useMock ? mockQuery : realQuery;
};

export const useAssetEventsData = ({
  assetId,
  limit,
  offset,
  orderBy,
  sourceDagId,
  sourceTaskId,
  timestampGte,
  timestampLte,
}: {
  assetId?: number;
  limit?: number;
  offset?: number;
  orderBy?: Array<string>;
  sourceDagId?: string;
  sourceTaskId?: string;
  timestampGte?: string;
  timestampLte?: string;
}) => {
  const useMock = mockEnabled();
  const realQuery = useAssetServiceGetAssetEvents(
    { assetId, limit, offset, orderBy, sourceDagId, sourceTaskId, timestampGte, timestampLte },
    undefined,
    { enabled: !useMock && assetId !== undefined },
  );
  const mockQuery = useQuery<AssetEventCollectionResponse>({
    enabled: useMock && assetId !== undefined,
    queryFn: async () =>
      new Promise<AssetEventCollectionResponse>((resolve, reject) => {
        setTimeout(() => {
          if (mockErrorEnabled()) {
            reject(new Error("Mock asset events request failed."));

            return;
          }

          const filteredEvents = mockAssetEvents.asset_events
            .filter((event) => event.asset_id === assetId)
            .filter((event) => (sourceDagId === undefined ? true : event.source_dag_id === sourceDagId))
            .filter((event) => (sourceTaskId === undefined ? true : event.source_task_id === sourceTaskId))
            .filter((event) => (timestampGte === undefined ? true : event.timestamp >= timestampGte))
            .filter((event) => (timestampLte === undefined ? true : event.timestamp <= timestampLte));
          const sortedEvents =
            orderBy?.[0] === "timestamp" ? [...filteredEvents].reverse() : [...filteredEvents];
          const start = offset ?? 0;
          const end = limit === undefined ? undefined : start + limit;

          resolve({
            asset_events: sortedEvents.slice(start, end),
            total_entries: filteredEvents.length,
          });
        }, 300);
      }),
    queryKey: [
      "mockAssetEvents",
      assetId,
      limit,
      offset,
      orderBy,
      sourceDagId,
      sourceTaskId,
      timestampGte,
      timestampLte,
    ],
  });

  return useMock ? mockQuery : realQuery;
};
