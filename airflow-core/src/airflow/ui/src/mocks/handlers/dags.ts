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

const successDag = {
  dag_display_name: "tutorial_taskflow_api_success",
  dag_id: "tutorial_taskflow_api_success",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/tutorial_taskflow_api.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_favorite: true,
  is_paused: false,
  is_stale: false,
  last_parsed_time: "2025-01-13T07:34:01.593459Z",
  latest_dag_runs: [
    {
      dag_id: "tutorial_taskflow_api",
      end_date: "2025-01-13T04:34:12.143831Z",
      id: 1,
      logical_date: "2025-01-13T04:33:58.396323Z",
      run_id: "manual__2025-01-13T04:33:58.387988+00:00",
      start_date: "2025-01-13T04:33:58.496197Z",
      state: "success",
    },
  ],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  owners: ["airflow"],
  pending_actions: [],
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
  is_favorite: false,
  is_paused: false,
  is_stale: false,
  last_parsed_time: "2025-01-13T07:34:01.593459Z",
  latest_dag_runs: [
    {
      dag_id: "tutorial_taskflow_api",
      end_date: "2025-01-13T04:34:12.143831Z",
      id: 2,
      logical_date: "2025-01-13T04:33:58.396323Z",
      run_id: "manual__2025-01-13T04:33:58.387988+00:00",
      start_date: "2025-01-13T04:33:58.496197Z",
      state: "success",
    },
  ],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  owners: ["airflow"],
  pending_actions: [],
  tags: [{ dag_id: "tutorial_taskflow_api_failed", name: "example" }],
  timetable_description: "Never, external triggers only",
};

const pausedDag = {
  dag_display_name: "paused_dag",
  dag_id: "paused_dag",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/paused_dag.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_favorite: false,
  is_paused: true,
  is_stale: false,
  last_parsed_time: "2025-01-13T07:34:01.593459Z",
  latest_dag_runs: [],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  owners: ["airflow"],
  pending_actions: [],
  tags: [{ dag_id: "paused_dag", name: "example" }],
  timetable_description: "Never, external triggers only",
};

const filterDagsByPaused = (paused: string | null) => {
  const allDags = [successDag, failedDag, pausedDag];

  if (paused === "true") {
    return allDags.filter((dag) => dag.is_paused);
  }
  if (paused === "false") {
    return allDags.filter((dag) => !dag.is_paused);
  }

  return allDags;
};

export const handlers: Array<HttpHandler> = [
  http.get("/ui/dags", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const paused = url.searchParams.get("paused");

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
    }

    const dags = filterDagsByPaused(paused);

    return HttpResponse.json({
      dags,
      total_entries: dags.length,
    });
  }),
  http.get("/api/v2/dags", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");

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
