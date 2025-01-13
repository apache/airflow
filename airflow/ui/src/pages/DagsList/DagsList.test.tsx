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
import "@testing-library/jest-dom";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServerApi } from "msw/node";
import { afterEach, describe, it, expect, beforeAll, afterAll } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

const handlers = [
  http.get("/ui/config", () =>
    HttpResponse.json({
      audit_view_excluded_events: "gantt,landing_times,tries,duration,calendar,graph,grid,tree,tree_data",
      audit_view_included_events: "",
      auto_refresh_interval: 3,
      default_ui_timezone: "UTC",
      default_wrap: false,
      enable_swagger_ui: true,
      hide_paused_dags_by_default: false,
      instance_name: "Airflow",
      instance_name_has_markup: false,
      is_k8s: false,
      navbar_color: "#fff",
      navbar_hover_color: "#eee",
      navbar_logo_text_color: "#51504f",
      navbar_text_color: "#51504f",
      navbar_text_hover_color: "#51504f",
      page_size: 15,
      require_confirmation_dag_change: false,
      state_color_mapping: {
        deferred: "mediumpurple",
        failed: "red",
        queued: "gray",
        removed: "lightgrey",
        restarting: "violet",
        running: "lime",
        scheduled: "tan",
        skipped: "hotpink",
        success: "green",
        up_for_reschedule: "turquoise",
        up_for_retry: "gold",
        upstream_failed: "orange",
      },
      test_connection: "Disabled",
      warn_deployment_exposure: true,
    }),
  ),
  http.get("/ui/dags/recent_dag_runs", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const successDag = {
      dag_display_name: "tutorial_taskflow_api_success",
      dag_id: "tutorial_taskflow_api_success",
      default_view: "grid",
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/home/karthikeyan/stuff/python/airflow/airflow/example_dags/tutorial_taskflow_api.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
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
      default_view: "grid",
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/home/karthikeyan/stuff/python/airflow/airflow/example_dags/tutorial_taskflow_api_failed.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
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
  http.get("/public/dags", ({ request }) => {
    const url = new URL(request.url);
    const lastDagRunState = url.searchParams.get("last_dag_run_state");
    const failedDag = {
      dag_display_name: "tutorial_taskflow_api_failed",
      dag_id: "tutorial_taskflow_api_failed",
      default_view: "grid",
      description: null,
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/home/karthikeyan/stuff/python/airflow/airflow/example_dags/tutorial_taskflow_api_failed.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
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
      default_view: "grid",
      description: null,
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/home/karthikeyan/stuff/python/airflow/airflow/example_dags/tutorial_taskflow_api_success.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
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

let server: SetupServerApi;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Dag Filters", () => {
  it("Filter by selected last run state", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByTestId("dags-success-filter")).toBeInTheDocument());
    await waitFor(() => screen.getByTestId("dags-success-filter").click());
    await waitFor(() => expect(screen.getByText(/tutorial_taskflow_api_success/iu)).toBeInTheDocument());

    await waitFor(() => expect(screen.getByTestId("dags-failed-filter")).toBeInTheDocument());
    await waitFor(() => screen.getByTestId("dags-failed-filter").click());
    await waitFor(() => expect(screen.getByText(/tutorial_taskflow_api_failed/iu)).toBeInTheDocument());
  });
});
