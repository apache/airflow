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
  http.get("/public/dags/tutorial_taskflow_api/details", () =>
    HttpResponse.json({
      asset_expression: null,
      catchup: false,
      concurrency: 16,
      dag_display_name: "tutorial_taskflow_api",
      dag_id: "tutorial_taskflow_api",
      dag_run_timeout: null,
      default_view: "grid",
      description: null,
      doc_md:
        "\n    ### TaskFlow API Tutorial Documentation\n    This is a simple data pipeline example which demonstrates the use of\n    the TaskFlow API using three simple tasks for Extract, Transform, and Load.\n    Documentation that goes along with the Airflow TaskFlow API tutorial is\n    located\n    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)\n    ",
      end_date: null,
      file_token:
        ".eJw9yUsOgCAMBcC7cAB7JPISizR82kCJcnvjxtUsJlDWxlQwPEvhjU7TV0pk27N2goxU9f7lB80qxxPXJF-uQ1CjY5avI0wO2-EFouohiw.fhdU5u0Pb7lElEd-AUUXqjHSsdo",
      fileloc: "/home/karthikeyan/stuff/python/airflow/airflow/example_dags/tutorial_taskflow_api.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
      is_paused_upon_creation: null,
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
    }),
  ),
];

let server: SetupServerApi;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Dag Documentation Modal", () => {
  it("Display documentation button only when docs_md is present", async () => {
    render(<AppWrapper initialEntries={["/dags/tutorial_taskflow_api"]} />);

    await waitFor(() => expect(screen.getByTestId("documentation-button")).toBeInTheDocument());
    await waitFor(() => screen.getByTestId("documentation-button").click());
    await waitFor(() =>
      expect(screen.getByText(/taskflow api tutorial documentation/iu)).toBeInTheDocument(),
    );
  });
});
