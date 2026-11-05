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

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

const baseTrigger: TriggerDetailResponse = {
  asset_watchers: [],
  callback: null,
  classpath: "airflow.providers.standard.triggers.temporal.DateTimeTrigger",
  consecutive_failures: 0,
  created_date: "2026-10-01T00:00:00Z",
  dag_id: null,
  first_failed_at: null,
  id: 0,
  kind: "task",
  last_error: null,
  last_event_at: null,
  last_failed_at: null,
  queue: null,
  status: "running",
  task_instance: null,
  team_name: null,
  triggerer_hostname: "triggerer-1",
};

// One trigger of each kind: a deferred task's, a deadline callback's and a failing asset watcher's.
export const triggers: Array<TriggerDetailResponse> = [
  {
    ...baseTrigger,
    dag_id: "deferring_dag",
    id: 1,
    kind: "task",
    task_instance: {
      dag_id: "deferring_dag",
      dag_run_id: "manual_run",
      map_index: -1,
      state: "deferred",
      task_id: "wait",
      try_number: 1,
    },
  },
  {
    ...baseTrigger,
    callback: {
      dag_id: "deadline_dag",
      dag_run_id: "scheduled_run",
      id: "0199a8e0-0000-7000-8000-000000000001",
      state: "pending",
    },
    classpath: "airflow.triggers.callback.CallbackTrigger",
    dag_id: "deadline_dag",
    id: 2,
    kind: "callback",
    status: "pending",
    triggerer_hostname: null,
  },
  {
    ...baseTrigger,
    asset_watchers: [
      {
        asset_id: 1,
        asset_name: "asset_with_dependencies",
        asset_uri: "s3://bucket/asset",
        watcher_name: "my_watcher",
      },
    ],
    classpath: "airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger",
    consecutive_failures: 3,
    first_failed_at: "2026-10-01T00:01:00Z",
    id: 3,
    kind: "asset_watcher",
    last_error: "ConnectionError: broker.example.com:9092 is unreachable",
    last_failed_at: "2026-10-01T00:04:00Z",
    status: "failing",
  },
];

export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/triggers", ({ request }) => {
    const params = new URL(request.url).searchParams;
    const kinds = params.getAll("kind");
    const statuses = params.getAll("status");
    const rows = triggers.filter(
      (trigger) =>
        (kinds.length === 0 || kinds.includes(trigger.kind)) &&
        (statuses.length === 0 || statuses.includes(trigger.status)),
    );

    return HttpResponse.json({ total_entries: rows.length, triggers: rows });
  }),
  http.get("/api/v2/triggers/:triggerId", ({ params }) => {
    const trigger = triggers.find(({ id }) => String(id) === params.triggerId);

    return trigger === undefined
      ? HttpResponse.json({ detail: "Not Found" }, { status: 404 })
      : HttpResponse.json(trigger);
  }),
];
