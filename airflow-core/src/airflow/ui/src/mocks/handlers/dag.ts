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

export const MOCK_DAG = {
  asset_expression: null,
  catchup: false,
  concurrency: 16,
  dag_display_name: "tutorial_taskflow_api",
  dag_id: "tutorial_taskflow_api",
  dag_run_timeout: null,
  description: null,
  doc_md:
    "\n    ### TaskFlow API Tutorial Documentation\n    This is a simple data pipeline example which demonstrates the use of\n    the TaskFlow API using three simple tasks for Extract, Transform, and Load.\n    Documentation that goes along with the Airflow TaskFlow API tutorial is\n    located\n    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)\n    ",
  end_date: null,
  file_token:
    ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
  fileloc: "/airflow/dags/tutorial_taskflow_api.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_paused: false,
  is_paused_upon_creation: null,
  is_stale: false,
  last_expired: null,
  last_parsed: "2025-01-13T04:33:54.141792Z",
  last_parsed_time: "2025-01-13T04:34:13.543097Z",
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  next_dagrun: null,
  next_dagrun_create_after: null,
  next_dagrun_data_interval_end: null,
  next_dagrun_data_interval_start: null,
  owners: ["airflow"],
  params: {},
  render_template_as_native_obj: false,
  start_date: "2021-01-01T00:00:00Z",
  tags: [{ dag_id: "tutorial_taskflow_api", name: "example" }],
  template_search_path: null,
  timetable_description: "Never, external triggers only",
  timetable_summary: null,
  timezone: "UTC",
};

const buildDag = (dagId: string) => ({ ...MOCK_DAG, dag_display_name: dagId, dag_id: dagId, tags: [] });

const buildTask = (taskId: string) => ({
  class_ref: null,
  depends_on_past: false,
  doc_md: null,
  downstream_task_ids: [],
  end_date: null,
  execution_timeout: null,
  extra_links: [],
  is_mapped: false,
  operator_name: "PythonOperator",
  owner: "airflow",
  params: {},
  pool: "default_pool",
  pool_slots: 1,
  priority_weight: 1,
  queue: "default",
  retries: 0,
  retry_delay: null,
  retry_exponential_backoff: 0,
  start_date: "2021-01-01T00:00:00Z",
  task_display_name: taskId,
  task_id: taskId,
  template_fields: [],
  trigger_rule: "all_success",
  ui_color: "#ffefeb",
  ui_fgcolor: "#000",
  wait_for_downstream: false,
  weight_rule: "downstream",
});

export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/dags/tutorial_taskflow_api/details", () => HttpResponse.json(MOCK_DAG)),
  http.get("/api/v2/dags/:dagId/details", ({ params }) => HttpResponse.json(buildDag(String(params.dagId)))),
  http.get("/api/v2/dags/:dagId", ({ params }) => HttpResponse.json(buildDag(String(params.dagId)))),
  http.get("/api/v2/dags/:dagId/tasks/:taskId", ({ params }) =>
    HttpResponse.json(buildTask(String(params.taskId))),
  ),
  http.get("/api/v2/dagTags", () =>
    HttpResponse.json({ tags: ["example", "example_tag", "other_tag"], total_entries: 3 }),
  ),
  http.get("/ui/dags/folders", () => HttpResponse.json({ folders: [], total_entries: 0 })),
  http.get("/ui/dags/:dagId/latest_run", () => HttpResponse.json(null)),
  http.get("/ui/dags/:dagId/deadlineAlerts", () =>
    HttpResponse.json({ deadline_alerts: [], total_entries: 0 }),
  ),
  // An empty grid: no runs, so the TI summaries stream is never opened by the grid itself.
  http.get("/ui/grid/runs/:dagId", () => HttpResponse.json([])),
  http.get("/ui/grid/structure/:dagId", () => HttpResponse.json([])),
  http.get(
    "/ui/grid/ti_summaries/:dagId",
    () => new HttpResponse("", { headers: { "Content-Type": "application/x-ndjson" } }),
  ),
  http.get("/ui/structure/structure_data", () => HttpResponse.json({ edges: [], nodes: [] })),
];
