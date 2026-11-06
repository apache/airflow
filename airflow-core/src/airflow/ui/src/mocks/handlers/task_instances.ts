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
import { http, HttpResponse, type HttpHandler, type PathParams } from "msw";

const buildTaskInstance = (params: PathParams, mapIndex: number) => {
  const dagId = String(params.dagId);
  const taskId = String(params.taskId);

  return {
    dag_display_name: dagId,
    dag_id: dagId,
    dag_run_id: String(params.dagRunId),
    dag_version: null,
    duration: 0.2,
    end_date: "2025-01-15T00:00:01Z",
    executor: null,
    executor_config: "{}",
    hostname: "worker",
    id: "01951900-16f6-7c1c-ae66-91bdfe9e0cfd",
    in_loop: false,
    logical_date: "2025-01-15T00:00:00Z",
    map_index: mapIndex,
    max_tries: 0,
    note: null,
    operator: "PythonOperator",
    operator_name: "PythonOperator",
    pid: 1234,
    pool: "default_pool",
    pool_slots: 1,
    priority_weight: 1,
    queue: "default",
    queued_when: "2025-01-15T00:00:00Z",
    rendered_fields: {},
    rendered_map_index: null,
    run_after: "2025-01-15T00:00:00Z",
    scheduled_when: "2025-01-15T00:00:00Z",
    start_date: "2025-01-15T00:00:00Z",
    state: "success",
    task_display_name: taskId,
    task_id: taskId,
    trigger: null,
    triggerer_job: null,
    try_number: 1,
    unixname: "airflow",
  };
};

const MAPPED_TASK_INSTANCE_PATH =
  /\/api\/v2\/dags\/([^/]+)\/dagRuns\/([^/]+)\/taskInstances\/([^/]+)\/(-?\d+)$/u;

// Registered after the more specific task instance handlers (e.g. src/mocks/handlers/log.ts),
// so these only answer what no fixture claims.
export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances", () =>
    HttpResponse.json({ task_instances: [], total_entries: 0 }),
  ),
  http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances/:taskId", ({ params }) =>
    HttpResponse.json(buildTaskInstance(params, -1)),
  ),
  http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances/:taskId/listMapped", () =>
    HttpResponse.json({ task_instances: [], total_entries: 0 }),
  ),
  http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances/:taskId/xcomEntries", () =>
    HttpResponse.json({ total_entries: 0, xcom_entries: [] }),
  ),
  // Matches integer map indexes only: a handler that declines with `undefined` lets msw pass the
  // request through to the network without reporting it as unhandled, so sub-resources such as
  // `/tries` or `/links` must not match here at all.
  http.get(MAPPED_TASK_INSTANCE_PATH, ({ request }) => {
    const [, dagId, dagRunId, taskId, mapIndex] =
      MAPPED_TASK_INSTANCE_PATH.exec(new URL(request.url).pathname) ?? [];

    return HttpResponse.json(
      buildTaskInstance(
        {
          dagId: decodeURIComponent(dagId ?? ""),
          dagRunId: decodeURIComponent(dagRunId ?? ""),
          taskId: decodeURIComponent(taskId ?? ""),
        },
        Number(mapIndex),
      ),
    );
  }),
];
