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

export const successDag = {
  dag_display_name: "tutorial_taskflow_api_success",
  dag_id: "tutorial_taskflow_api_success",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/tutorial_taskflow_api.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  has_unfinished_runs: false,
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
      run_after: "2025-01-13T04:33:58.396323Z",
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
  timetable_type: "NullTimetable",
};

export const failedDag = {
  dag_display_name: "tutorial_taskflow_api_failed",
  dag_id: "tutorial_taskflow_api_failed",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/tutorial_taskflow_api_failed.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  has_unfinished_runs: false,
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
      run_after: "2025-01-13T04:33:58.396323Z",
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
  timetable_type: "CronTriggerTimetable",
};

export const pausedDag = {
  dag_display_name: "paused_dag",
  dag_id: "paused_dag",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/paused_dag.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  has_unfinished_runs: false,
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
  timetable_type: "NullTimetable",
};

export const drainingDag = {
  dag_display_name: "draining_dag",
  dag_id: "draining_dag",
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/draining_dag.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  has_unfinished_runs: true,
  is_favorite: false,
  is_paused: false,
  is_stale: false,
  last_parsed_time: "2025-01-13T07:34:01.593459Z",
  latest_dag_runs: [],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  owners: ["airflow"],
  pending_actions: [],
  scheduling_state: "draining",
  tags: [{ dag_id: "draining_dag", name: "example" }],
  timetable_description: "Never, external triggers only",
  timetable_type: "NullTimetable",
};

// Failed to import, so the Dags list has something for the import-errors filter to single out.
export const brokenDag = {
  ...pausedDag,
  dag_display_name: "broken_dag",
  dag_id: "broken_dag",
  has_import_errors: true,
  is_paused: false,
  tags: [{ dag_id: "broken_dag", name: "example" }],
};

export const defaultDags = [successDag, failedDag, pausedDag];

const filterDags = ({
  dags,
  hasImportErrors,
  paused,
  schedulingState,
  timetableTypes,
}: {
  dags: Array<(typeof defaultDags)[number] | typeof brokenDag | typeof drainingDag>;
  hasImportErrors: string | null;
  paused: string | null;
  schedulingState: string | null;
  timetableTypes: Array<string>;
}) => {
  let allDags = dags;

  if (hasImportErrors !== null) {
    allDags = allDags.filter((dag) => dag.has_import_errors === (hasImportErrors === "true"));
  }

  if (timetableTypes.length > 0) {
    allDags = allDags.filter((dag) => timetableTypes.includes(dag.timetable_type));
  }

  // Narrower than ``paused=false``: a draining Dag is unpaused, so the two are not alternatives.
  if (schedulingState === "draining") {
    allDags = allDags.filter((dag) => "scheduling_state" in dag && dag.scheduling_state === "draining");
  }

  if (paused === "true") {
    return allDags.filter((dag) => dag.is_paused);
  }

  return paused === "false" ? allDags.filter((dag) => !dag.is_paused) : allDags;
};

/**
 * ``/ui/dags`` over a caller-chosen fixture, so a suite can add the Dags it needs without
 * enlarging the set every other suite renders.
 */
export const dagsListHandler = (
  dags: Array<(typeof defaultDags)[number] | typeof brokenDag | typeof drainingDag> = defaultDags,
) =>
  http.get("/ui/dags", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const anyDagRunState = url.searchParams.get("dag_run_state");
    const orderBy = url.searchParams.get("order_by");
    const hasImportErrors = url.searchParams.get("has_import_errors");
    const paused = url.searchParams.get("paused");
    const schedulingState = url.searchParams.get("scheduling_state");
    const timetableTypes = url.searchParams.getAll("timetable_type");

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

    // Any-run matching: pretend every Dag has a failed run somewhere in its history, so
    // widening the pill's lookback from the latest run visibly changes the result set.
    if (anyDagRunState === "failed") {
      return HttpResponse.json({
        dags: [failedDag, successDag],
        total_entries: 2,
      });
    }

    let filtered = filterDags({ dags, hasImportErrors, paused, schedulingState, timetableTypes });

    if (orderBy === "last_run_run_after" || orderBy === "-last_run_run_after") {
      filtered = [failedDag, successDag, pausedDag, drainingDag].filter((dag) => filtered.includes(dag));
    }

    return HttpResponse.json({
      dags: filtered,
      total_entries: filtered.length,
    });
  });

export const handlers: Array<HttpHandler> = [
  http.get("/ui/dags/timetable_types", ({ request }) => {
    const url = new URL(request.url);
    const prefix = url.searchParams.get("timetable_type_prefix_pattern") ?? "";
    const timetableTypes = ["CronTriggerTimetable", "NullTimetable"].filter((timetableType) =>
      timetableType.startsWith(prefix),
    );

    return HttpResponse.json({
      timetable_types: timetableTypes,
      total_entries: timetableTypes.length,
    });
  }),
  dagsListHandler(),
  http.get("/ui/dags/run_state_counts", () => HttpResponse.json({ dags: [], state_count_limit: 1000 })),
  http.get("/ui/dags/recent_task_instance_state_counts", () => HttpResponse.json({ dags: [] })),
  http.get("/ui/teams", () => HttpResponse.json({ teams: [], total_entries: 0 })),
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
