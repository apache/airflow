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

import type {
  DashboardDagStatsResponse,
  HealthInfoResponse,
  HistoricalMetricDataResponse,
} from "openapi/requests/types.gen";

// The Dashboard's own queries, which its route loader also prefetches whenever a test renders the app root.
const EMPTY_HISTORICAL_METRICS: HistoricalMetricDataResponse = {
  dag_run_states: { failed: 0, queued: 0, running: 0, success: 0 },
  task_instance_states: {
    awaiting_input: 0,
    deferred: 0,
    failed: 0,
    no_status: 0,
    queued: 0,
    removed: 0,
    restarting: 0,
    running: 0,
    scheduled: 0,
    skipped: 0,
    success: 0,
    up_for_reschedule: 0,
    up_for_retry: 0,
    upstream_failed: 0,
  },
};

const NO_DAG_STATS: DashboardDagStatsResponse = {
  active_dag_count: 0,
  failed_dag_count: 0,
  queued_dag_count: 0,
  running_dag_count: 0,
};

const HEALTHY: HealthInfoResponse = {
  dag_processor: { detailed_status: null, latest_dag_processor_heartbeat: null, status: "healthy" },
  metadatabase: { status: "healthy" },
  scheduler: { detailed_status: null, latest_scheduler_heartbeat: null, status: "healthy" },
  triggerer: { detailed_status: null, latest_triggerer_heartbeat: null, status: "healthy" },
};

export const handlers: Array<HttpHandler> = [
  http.get("/ui/dashboard/historical_metrics_data", () => HttpResponse.json(EMPTY_HISTORICAL_METRICS)),
  http.get("/ui/dashboard/dag_stats", () => HttpResponse.json(NO_DAG_STATS)),
  http.get("/api/v2/monitor/health", () => HttpResponse.json(HEALTHY)),
];
