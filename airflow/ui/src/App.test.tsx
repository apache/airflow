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
import type { QueryObserverSuccessResult } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import { afterEach, beforeEach, describe, it, vi } from "vitest";

import * as openapiQueriesModule from "openapi/queries";
import type { DAGCollection } from "openapi/requests/types.gen";

import { AppSimple } from "./AppSimple";
import { App } from "./App";
import { Wrapper } from "./utils/Wrapper";

const mockListDags: DAGCollection = {
  dags: [
    {
      dag_display_name: "nested_groups",
      dag_id: "nested_groups",
      default_view: "grid",
      description: null,
      file_token:
        "Ii9maWxlcy9kYWdzL25lc3RlZF90YXNrX2dyb3Vwcy5weSI.G3EkdxmDUDQsVb7AIZww1TSGlFE",
      fileloc: "/files/dags/nested_task_groups.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
      last_expired: null,
      last_parsed_time: "2024-08-22T13:50:10.372238+00:00",
      last_pickled: null,
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      next_dagrun: "2024-08-22T00:00:00+00:00",
      next_dagrun_create_after: "2024-08-23T00:00:00+00:00",
      next_dagrun_data_interval_end: "2024-08-23T00:00:00+00:00",
      next_dagrun_data_interval_start: "2024-08-22T00:00:00+00:00",
      owners: ["airflow"],
      pickle_id: null,
      scheduler_lock: null,
      tags: [],
      timetable_description: "",
    },
    {
      dag_display_name: "simple_bash_operator",
      dag_id: "simple_bash_operator",
      default_view: "grid",
      description: null,
      file_token:
        "Ii9maWxlcy9kYWdzL3NpbXBsZV9iYXNoX29wZXJhdG9yLnB5Ig.RteaxTC78ceHlgMkfU3lfznlcLI",
      fileloc: "/files/dags/simple_bash_operator.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_active: true,
      is_paused: false,
      last_expired: null,
      last_parsed_time: "2024-08-22T13:50:10.368561+00:00",
      last_pickled: null,
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      next_dagrun: "2024-08-22T00:00:00+00:00",
      next_dagrun_create_after: "2024-08-23T00:00:00+00:00",
      next_dagrun_data_interval_end: "2024-08-23T00:00:00+00:00",
      next_dagrun_data_interval_start: "2024-08-22T00:00:00+00:00",
      owners: ["airflow"],
      pickle_id: null,
      scheduler_lock: null,
      tags: [
        {
          name: "example2",
        },
        {
          name: "example",
        },
      ],
      timetable_description: "At 00:00",
    },
  ],
  total_entries: 2,
};

beforeEach(() => {
  const returnValue = {
    data: mockListDags,
    isLoading: false,
  } as QueryObserverSuccessResult<DAGCollection, unknown>;

  vi.spyOn(openapiQueriesModule, "useDagServiceGetDags").mockImplementation(
    () => returnValue,
  );
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("App", () => {
  it("App component should render", () => {
    render(<App />, { wrapper: Wrapper });
  });

  it("AppSimple", () => {
    render(<AppSimple />);
  });
});
