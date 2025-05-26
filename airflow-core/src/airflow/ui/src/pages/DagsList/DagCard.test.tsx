/* eslint-disable unicorn/no-null */

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
import { render, screen } from "@testing-library/react";
import type { DagTagResponse, DAGWithLatestDagRunsResponse } from "openapi-gen/requests/types.gen";
import { afterEach, describe, it, vi, expect } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { DagCard } from "./DagCard";

const mockDag = {
  asset_expression: null,
  bundle_name: "dags-folder",
  bundle_version: "1",
  dag_display_name: "nested_groups",
  dag_id: "nested_groups",
  description: null,
  file_token: "Ii9maWxlcy9kYWdzL25lc3RlZF90YXNrX2dyb3Vwcy5weSI.G3EkdxmDUDQsVb7AIZww1TSGlFE",
  fileloc: "/files/dags/nested_task_groups.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_paused: false,
  is_stale: false,
  last_expired: null,
  last_parsed_time: "2024-08-22T13:50:10.372238+00:00",
  latest_dag_runs: [],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  next_dagrun_data_interval_end: "2024-08-23T00:00:00+00:00",
  next_dagrun_data_interval_start: "2024-08-22T00:00:00+00:00",
  next_dagrun_logical_date: "2024-08-22T00:00:00+00:00",
  next_dagrun_run_after: "2024-08-23T00:00:00+00:00",
  owners: ["airflow"],
  relative_fileloc: "nested_task_groups.py",
  tags: [],
  timetable_description: "",
  timetable_summary: "",
} satisfies DAGWithLatestDagRunsResponse;

afterEach(() => {
  vi.restoreAllMocks();
});

describe("DagCard", () => {
  it("DagCard should render without tags", () => {
    render(<DagCard dag={mockDag} />, { wrapper: Wrapper });
    expect(screen.getByText(mockDag.dag_display_name)).toBeInTheDocument();
    expect(screen.queryByTestId("dag-tag")).toBeNull();
  });

  it("DagCard should not show +X more text if there is only +1 over the limit", () => {
    const tags = [
      { dag_id: "id", name: "tag1" },
      { dag_id: "id", name: "tag2" },
      { dag_id: "id", name: "tag3" },
      { dag_id: "id", name: "tag4" },
    ] satisfies Array<DagTagResponse>;

    const expandedMockDag = {
      ...mockDag,
      tags,
    } satisfies DAGWithLatestDagRunsResponse;

    render(<DagCard dag={expandedMockDag} />, { wrapper: Wrapper });
    expect(screen.getByTestId("dag-tag")).toBeInTheDocument();
    expect(screen.queryByText("tag3")).toBeInTheDocument();
    expect(screen.queryByText("tag4")).toBeInTheDocument();
    expect(screen.queryByText(", +1 more")).toBeNull();
  });

  it("DagCard should show +X more text if there are more than 3 tags", () => {
    const tags = [
      { dag_id: "id", name: "tag1" },
      { dag_id: "id", name: "tag2" },
      { dag_id: "id", name: "tag3" },
      { dag_id: "id", name: "tag4" },
      { dag_id: "id", name: "tag5" },
    ] satisfies Array<DagTagResponse>;

    const expandedMockDag = {
      ...mockDag,
      tags,
    } satisfies DAGWithLatestDagRunsResponse;

    render(<DagCard dag={expandedMockDag} />, { wrapper: Wrapper });
    expect(screen.getByTestId("dag-tag")).toBeInTheDocument();
    expect(screen.getByText("+2 more")).toBeInTheDocument();
  });
});
