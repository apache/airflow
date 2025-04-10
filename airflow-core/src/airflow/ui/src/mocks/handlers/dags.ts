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

/* eslint-disable unicorn/no-null */
import { http, HttpResponse, type HttpHandler } from "msw";

export const handlers: Array<HttpHandler> = [
  http.get("/ui/dags/recent_dag_runs", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const successDag = {
      dag_display_name: "tutorial_taskflow_api_success",
      dag_id: "tutorial_taskflow_api_success",
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/airflow/dags/tutorial_taskflow_api.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_paused: false,
      is_stale: false,
      last_parsed_time: "2025-01-13T07:34:01.593459Z",
      latest_dag_runs: [
        {
          conf: {},
          dag_id: "tutorial_taskflow_api",
          dag_run_id: "manual__2025-01-13T04:33:58.387988+00:00",
          data_interval_end: "2025-01-13T04:33:58.396323Z",
          data_interval_start: "2025-01-13T04:33:58.396323Z",
          end_date: "2025-01-13T04:34:12.143831Z",
          external_trigger: true,
          last_scheduling_decision: "2025-01-13T04:34:12.137382Z",
          logical_date: "2025-01-13T04:33:58.396323Z",
          queued_at: "2025-01-13T04:33:58.404628Z",
          run_type: "manual",
          start_date: "2025-01-13T04:33:58.496197Z",
          state: "success",
          triggered_by: "rest_api",
        },
      ],
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      owners: ["airflow"],
      tags: [{ dag_id: "tutorial_taskflow_api_success", name: "example" }],
      timetable_description: "Never, external triggers only",
    };
    const failedDag = {
      dag_display_name: "tutorial_taskflow_api_failed",
      dag_id: "tutorial_taskflow_api_failed",
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/airflow/dags/tutorial_taskflow_api_failed.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_paused: false,
      is_stale: false,
      last_parsed_time: "2025-01-13T07:34:01.593459Z",
      latest_dag_runs: [
        {
          conf: {},
          dag_id: "tutorial_taskflow_api",
          dag_run_id: "manual__2025-01-13T04:33:58.387988+00:00",
          data_interval_end: "2025-01-13T04:33:58.396323Z",
          data_interval_start: "2025-01-13T04:33:58.396323Z",
          end_date: "2025-01-13T04:34:12.143831Z",
          external_trigger: true,
          last_scheduling_decision: "2025-01-13T04:34:12.137382Z",
          logical_date: "2025-01-13T04:33:58.396323Z",
          queued_at: "2025-01-13T04:33:58.404628Z",
          run_type: "manual",
          start_date: "2025-01-13T04:33:58.496197Z",
          state: "success",
          triggered_by: "rest_api",
        },
      ],
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      owners: ["airflow"],
      tags: [{ dag_id: "tutorial_taskflow_api_failed", name: "example" }],
      timetable_description: "Never, external triggers only",
    };

    if (lastDagRunState === "success") {
      return HttpResponse.json({
        dags: [successDag],
        total_entries: 1,
      });
    } else if (lastDagRunState === "failed") {
      return HttpResponse.json({
        dags: [failedDag],
        total_entries: 1,
      });
    } else {
      return HttpResponse.json({
        dags: [failedDag],
        total_entries: 1,
      });
    }
  }),
  http.get("/api/v2/dags", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const failedDag = {
      dag_display_name: "tutorial_taskflow_api_failed",
      dag_id: "tutorial_taskflow_api_failed",
      description: null,
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/airflow/dags/tutorial_taskflow_api_failed.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_paused: false,
      is_stale: false,
      last_expired: null,
      last_parsed_time: "2025-01-13T06:45:33.009609Z",
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      next_dagrun: null,
      next_dagrun_create_after: null,
      next_dagrun_data_interval_end: null,
      next_dagrun_data_interval_start: null,
      owners: ["airflow"],
      tags: [{ dag_id: "tutorial_taskflow_api_failed", name: "example" }],
      timetable_description: "Never, external triggers only",
      timetable_summary: null,
    };

    const successDag = {
      dag_display_name: "tutorial_taskflow_api_success",
      dag_id: "tutorial_taskflow_api_success",
      description: null,
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/airflow/dags/tutorial_taskflow_api_success.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_paused: false,
      is_stale: false,
      last_expired: null,
      last_parsed_time: "2025-01-13T06:45:33.009609Z",
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      next_dagrun: null,
      next_dagrun_create_after: null,
      next_dagrun_data_interval_end: null,
      next_dagrun_data_interval_start: null,
      owners: ["airflow"],
      tags: [{ dag_id: "tutorial_taskflow_api_success", name: "example" }],
      timetable_description: "Never, external triggers only",
      timetable_summary: null,
    };

    if (lastDagRunState === "failed") {
      return HttpResponse.json({
        dags: [failedDag],
        total_entries: 1,
      });
    } else if (lastDagRunState === "success") {
      return HttpResponse.json({
        dags: [successDag],
        total_entries: 1,
      });
    } else {
      return HttpResponse.json({
        dags: [failedDag, successDag],
        total_entries: 2,
      });
    }
  }),
];
