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

/* eslint-disable unicorn/no-null,max-lines */
import { http, HttpResponse, type HttpHandler } from "msw";

const ti = {
  dag_id: "log_grouping",
  dag_run_id: "<OVERRIDE_BELOW>",
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
  unixname: "testname",
};

export const handlers: Array<HttpHandler> = [
  http.get("/api/v2/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/generate/-1", () =>
    HttpResponse.json({ ...ti, dag_run_id: "manual__2025-02-18T12:19" }),
  ),
  http.get("/api/v2/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/generate/logs/1", () =>
    HttpResponse.json({
      content: [
        {
          event: "::group::Log message source details",
          sources: [
            "/home/airflow/logs/dag_id=tutorial_dag/run_id=manual__2025-02-28T05:18:54.249762+00:00/task_id=load/attempt=1.log",
          ],
        },
        { event: "::endgroup::" },
        {
          event:
            "[2025-02-28T10:49:09.535+0530] {local_task_job_runner.py:120} INFO - ::group::Pre task execution logs",
          timestamp: "2025-02-28T10:49:09.535000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.674+0530] {taskinstance.py:2348} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: tutorial_dag.load manual__2025-02-28T05:18:54.249762+00:00 [queued]>",
          timestamp: "2025-02-28T10:49:09.674000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.678+0530] {taskinstance.py:2348} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: tutorial_dag.load manual__2025-02-28T05:18:54.249762+00:00 [queued]>",
          timestamp: "2025-02-28T10:49:09.678000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.679+0530] {taskinstance.py:2589} INFO - Starting attempt 1 of 3",
          timestamp: "2025-02-28T10:49:09.679000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.697+0530] {taskinstance.py:2612} INFO - Executing <Task(PythonOperator): load> on 2025-02-25 06:42:00+00:00",
          timestamp: "2025-02-28T10:49:09.697000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.704+0530] {standard_task_runner.py:131} INFO - Started process 24882 to run task",
          timestamp: "2025-02-28T10:49:09.704000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.706+0530] {standard_task_runner.py:147} INFO - Running: ['airflow', 'tasks', 'run', 'tutorial_dag', 'load', 'manual__2025-02-28T05:18:54.249762+00:00', '--raw', '--subdir', '/home/airflow/airflow/example_dags/tutorial_dag.py', '--cfg-path', '/tmp/tmpglv7rpjo']",
          timestamp: "2025-02-28T10:49:09.706000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.707+0530] {standard_task_runner.py:148} INFO - Subtask load",
          timestamp: "2025-02-28T10:49:09.707000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.740+0530] {task_command.py:442} INFO - Running <TaskInstance: tutorial_dag.load manual__2025-02-28T05:18:54.249762+00:00 [running]> on host laptop",
          timestamp: "2025-02-28T10:49:09.740000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.841+0530] {taskinstance.py:2897} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='airflow' AIRFLOW_CTX_DAG_ID='tutorial_dag' AIRFLOW_CTX_TASK_ID='load' AIRFLOW_CTX_LOGICAL_DATE='2025-02-25T06:42:00+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2025-02-28T05:18:54.249762+00:00'",
          timestamp: "2025-02-28T10:49:09.841000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.842+0530] {logging_mixin.py:212} INFO - Task instance is in running state",
          timestamp: "2025-02-28T10:49:09.842000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.842+0530] {logging_mixin.py:212} INFO -  Previous state of the Task instance: queued",
          timestamp: "2025-02-28T10:49:09.842000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.848+0530] {logging_mixin.py:212} INFO - Current task name:load",
          timestamp: "2025-02-28T10:49:09.848000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.848+0530] {logging_mixin.py:212} INFO - Dag name:tutorial_dag",
          timestamp: "2025-02-28T10:49:09.848000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.848+0530] {taskinstance.py:689} INFO - ::endgroup::",
          timestamp: "2025-02-28T10:49:09.848000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.852+0530] {logging_mixin.py:212} INFO - {'total_order_value': 1236.7}",
          timestamp: "2025-02-28T10:49:09.852000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.852+0530] {python.py:198} INFO - Done. Returned value was: None",
          timestamp: "2025-02-28T10:49:09.852000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.856+0530] {taskinstance.py:335} INFO - ::group::Post task execution logs",
          timestamp: "2025-02-28T10:49:09.856000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.856+0530] {taskinstance.py:347} INFO - Marking task as SUCCESS. dag_id=tutorial_dag, task_id=load, run_id=manual__2025-02-28T05:18:54.249762+00:00, logical_date=20250225T064200, start_date=20250228T051909, end_date=20250228T051909",
          timestamp: "2025-02-28T10:49:09.856000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.871+0530] {logging_mixin.py:212} INFO - Task instance in success state",
          timestamp: "2025-02-28T10:49:09.871000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.872+0530] {logging_mixin.py:212} INFO -  Previous state of the Task instance: running",
          timestamp: "2025-02-28T10:49:09.872000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.875+0530] {logging_mixin.py:212} INFO - Task operator:<Task(PythonOperator): load>",
          timestamp: "2025-02-28T10:49:09.875000+05:30",
        },
        {
          event:
            "[2025-02-28T10:49:09.920+0530] {local_task_job_runner.py:262} INFO - Task exited with return code 0",
          timestamp: "2025-02-28T10:49:09.920000+05:30",
        },
        {
          event: "[2025-02-28T10:49:09.920+0530] {local_task_job_runner.py:241} INFO - ::endgroup::",
          timestamp: "2025-02-28T10:49:09.920000+05:30",
        },
      ],
      continuation_token: null,
    }),
  ),
  http.get("/api/v2/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/log_source/-1", () =>
    HttpResponse.json({
      ...ti,
      dag_run_id: "manual__2025-02-18T12:19",
      task_display_name: "log_source",
      task_id: "log_source",
    }),
  ),
  http.get("/api/v2/dags/log_grouping/dagRuns/manual__2025-02-18T12:19/taskInstances/log_source/logs/1", () =>
    HttpResponse.json({
      content: [
        {
          event: "::group::Log message source details",
          sources: [
            "/root/airflow/logs/dag_id=log_grouping/run_id=manual__2025-02-18T12:19/task_id=log_source/attempt=1.log",
          ],
          timestamp: null,
        },
        { event: "::endgroup::", timestamp: null },
        {
          event: "DAG bundles loaded: dags-folder, example_dags",
          filename: "manager.py",
          level: "info",
          lineno: 179,
          logger: "airflow.dag_processing.bundles.manager.DagBundlesManager",
          timestamp: "2025-09-11T17:44:52.567095Z",
        },
        {
          event:
            "Filling up the DagBag from /opt/airflow/airflow-core/src/airflow/example_dags/standard/example_python_decorator.py",
          filename: "dagbag.py",
          level: "info",
          lineno: 593,
          logger: "airflow.models.dagbag.DagBag",
          timestamp: "2025-09-11T17:44:52.567407Z",
        },
        {
          event: "Task instance is in running state",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597476Z",
        },
        {
          event: " Previous state of the Task instance: TaskInstanceState.QUEUED",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597589Z",
        },
        {
          event: "Current task name:log_sql_query",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597626Z",
        },
        {
          event: "Dag name:example_python_decorator",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.597657Z",
        },
        {
          event:
            "Python task decorator query: CREATE TABLE Orders (\n    order_id INT PRIMARY KEY,\n    name TEXT,\n    description TEXT\n)",
          filename: "example_python_decorator.py",
          level: "info",
          lineno: 60,
          logger: "unusual_prefix_7bb6f64024e819254594fca018f7123719f205f5_example_python_decorator",
          timestamp: "2025-09-11T17:44:52.598196Z",
        },
        {
          event: "Done. Returned value was: None",
          filename: "python.py",
          level: "info",
          lineno: 218,
          logger:
            "airflow.task.operators.airflow.providers.standard.decorators.python._PythonDecoratedOperator",
          timestamp: "2025-09-11T17:44:52.598300Z",
        },
        {
          event: "Task instance in success state",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.607859Z",
        },
        {
          event: " Previous state of the Task instance: TaskInstanceState.RUNNING",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.607929Z",
        },
        {
          event: "Task operator:<Task(_PythonDecoratedOperator): log_sql_query>",
          level: "info",
          logger: "task.stdout",
          timestamp: "2025-09-11T17:44:52.607985Z",
        },
      ],
      continuation_token: null,
    }),
  ),
];
