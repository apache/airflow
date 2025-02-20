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
  http.get("/public/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/generate/-1", () =>
    HttpResponse.json({
      dag_id: "log_grouping",
      dag_run_id: "manual__2025-02-18T12:19",
      dag_version: {
        bundle_name: "dags-folder",
        bundle_url: null,
        bundle_version: null,
        created_at: "2025-02-18T12:06:45.723238Z",
        dag_id: "log_grouping",
        id: "019518f4-1adb-7223-a917-45fe08b78947",
        version_number: 1,
      },
      duration: 0.203_977,
      end_date: "2025-02-18T12:19:56.467235Z",
      executor: null,
      executor_config: "{}",
      hostname: "laptop",
      id: "01951900-16f6-7c1c-ae66-91bdfe9e0cfd",
      logical_date: null,
      map_index: -1,
      max_tries: 0,
      note: null,
      operator: "_PythonDecoratedOperator",
      pid: 20_703,
      pool: "default_pool",
      pool_slots: 1,
      priority_weight: 1,
      queue: "default",
      queued_when: "2025-02-18T12:19:52.311873Z",
      rendered_fields: { op_args: "()", op_kwargs: {}, templates_dict: null },
      rendered_map_index: null,
      run_after: "2025-02-18T12:19:51.120210Z",
      scheduled_when: "2025-02-18T12:19:52.289327Z",
      start_date: "2025-02-18T12:19:56.263258Z",
      state: "success",
      task_display_name: "generate",
      task_id: "generate",
      trigger: null,
      triggerer_job: null,
      try_number: 1,
      unixname: "karthikeyan",
    }),
  ),
  http.get("/public/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/generate/logs/1", () =>
    HttpResponse.json({
      content:
        "[2025-02-18T17:49:56.462+0530] {logging_mixin.py:212} INFO - ::group::group name\\n[2025-02-18T17:49:56.462+0530] {logging_mixin.py:212} INFO - ::group::inner group name 0\\n[2025-02-18T17:49:56.462+0530] {logging_mixin.py:212} INFO - c3dacfc301094ebbaf02172dd4808e82\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - a6d1fa3bd57c4d14a3767afa8a0a448c\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - ::endgroup::\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - ::group::inner group name 1\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - 8cd74c3beba14447a3f7cfb929b18df5\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - f5ff1c82aad048c2b9c64a349b274305\\n[2025-02-18T17:49:56.463+0530] {logging_mixin.py:212} INFO - ::endgroup::",
      continuation_token: "eyJlbmRfb2ZfbG9nIjp0cnVlLCJsb2dfcG9zIjozODM3fQ.8qgLbxyEzr1Z4ruegn2QGTUFJRA",
    }),
  ),
];
