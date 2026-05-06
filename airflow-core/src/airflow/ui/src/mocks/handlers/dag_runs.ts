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

const dagRunBeforeFilter = {
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_before_filter",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: 1.5,
  end_date: "2024-12-31T00:00:01Z",
  logical_date: "2024-12-31T00:00:00Z",
  partition_key: null,
  run_after: "2024-12-31T00:00:00Z",
  run_type: "manual",
  start_date: "2024-12-31T00:00:00Z",
  state: "success",
  triggering_user_name: "admin",
};

const dagRunInRange = {
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_in_range",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: 2.0,
  end_date: "2025-01-15T00:00:01Z",
  logical_date: "2025-01-15T00:00:00Z",
  partition_key: null,
  run_after: "2025-01-15T00:00:00Z",
  run_type: "manual",
  start_date: "2025-01-15T00:00:00Z",
  state: "success",
  triggering_user_name: "admin",
};

export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/dags/:dagId/dagRuns", ({ request }) => {
    const url = new URL(request.url);
    const logicalDateGte = url.searchParams.get("logical_date_gte");
    const logicalDateLte = url.searchParams.get("logical_date_lte");

    const allRuns = [dagRunBeforeFilter, dagRunInRange];

    const filtered = allRuns.filter((run) => {
      const logicalDate = new Date(run.logical_date);

      if (logicalDateGte !== null && logicalDate < new Date(logicalDateGte)) {
        return false;
      }
      if (logicalDateLte !== null && logicalDate > new Date(logicalDateLte)) {
        return false;
      }

      return true;
    });

    return HttpResponse.json({
      dag_runs: filtered,
      total_entries: filtered.length,
    });
  }),
];
