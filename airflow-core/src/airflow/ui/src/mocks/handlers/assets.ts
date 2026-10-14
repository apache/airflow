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
import { http, HttpResponse, type HttpHandler } from "msw";

const assetWithDependencies = {
  aliases: [{ group: "alias_group", id: 1, name: "my_alias" }],
  consuming_tasks: [{ created_at: "2025-01-01T00:00:00Z", dag_id: "consumer_dag", task_id: "consumer_task" }],
  created_at: "2025-01-01T00:00:00Z",
  extra: { owner: "data_team" },
  group: "asset_group",
  id: 1,
  last_asset_event: { id: 10, timestamp: "2025-01-15T00:00:00Z" },
  name: "asset_with_dependencies",
  producing_tasks: [],
  scheduled_dags: [],
  updated_at: "2025-01-02T00:00:00Z",
  uri: "s3://bucket/asset",
  watchers: [{ created_date: "2025-01-01T00:00:00Z", name: "my_watcher", trigger_id: 5 }],
};

const plainAsset = {
  ...assetWithDependencies,
  aliases: [],
  id: 2,
  name: "plain_asset",
  watchers: [],
};

export const handlers: Array<HttpHandler> = [
  http.get("/ui/assets", () => HttpResponse.json({ assets: [assetWithDependencies], total_entries: 1 })),
  http.get("/api/v2/assets/:assetId", ({ params }) =>
    HttpResponse.json(params.assetId === "2" ? plainAsset : assetWithDependencies),
  ),
];
