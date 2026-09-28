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

const backfillBeforeFilter = {
  completed_at: "2024-12-31T01:00:00Z",
  created_at: "2024-12-31T00:00:00Z",
  dag_display_name: "tutorial_taskflow_api",
  dag_id: "tutorial_taskflow_api",
  dag_run_conf: {},
  from_date: "2024-12-01T00:00:00Z",
  id: 1,
  is_paused: false,
  max_active_runs: 10,
  reprocess_behavior: "none",
  to_date: "2024-12-31T00:00:00Z",
  updated_at: "2024-12-31T01:00:00Z",
};

const backfillInRange = {
  completed_at: "2025-01-16T01:00:00Z",
  created_at: "2025-01-16T00:00:00Z",
  dag_display_name: "tutorial_taskflow_api",
  dag_id: "tutorial_taskflow_api",
  dag_run_conf: {},
  from_date: "2025-01-01T00:00:00Z",
  id: 2,
  is_paused: false,
  max_active_runs: 10,
  reprocess_behavior: "none",
  to_date: "2025-01-15T00:00:00Z",
  updated_at: "2025-01-16T01:00:00Z",
};

export const handlers: Array<HttpHandler> = [
  http.get("/ui/backfills", ({ request }) => {
    const url = new URL(request.url);
    const fromDateGte = url.searchParams.get("from_date_gte");
    const fromDateLte = url.searchParams.get("from_date_lte");
    const toDateGte = url.searchParams.get("to_date_gte");
    const toDateLte = url.searchParams.get("to_date_lte");

    const allBackfills = [backfillBeforeFilter, backfillInRange];

    const filtered = allBackfills.filter((backfill) => {
      const fromDate = new Date(backfill.from_date);
      const toDate = new Date(backfill.to_date);

      if (fromDateGte !== null && fromDate < new Date(fromDateGte)) {
        return false;
      }
      if (fromDateLte !== null && fromDate > new Date(fromDateLte)) {
        return false;
      }
      if (toDateGte !== null && toDate < new Date(toDateGte)) {
        return false;
      }
      if (toDateLte !== null && toDate > new Date(toDateLte)) {
        return false;
      }

      return true;
    });

    return HttpResponse.json({
      backfills: filtered,
      total_entries: filtered.length,
    });
  }),
];
