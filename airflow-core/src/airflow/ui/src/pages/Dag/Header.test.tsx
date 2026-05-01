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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import type { DAGDetailsResponse } from "openapi-gen/requests/types.gen";
import { describe, expect, it } from "vitest";

import i18n from "src/i18n/config";
import { MOCK_DAG } from "src/mocks/handlers/dag";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

const mockDag = {
  ...MOCK_DAG,
  active_runs_count: 0,
  allowed_run_types: [],
  bundle_name: "dags-folder",
  bundle_version: "1",
  default_args: {},
  fileloc: "/files/dags/stale_dag.py",
  is_favorite: false,
  is_stale: true,
  last_parse_duration: 0.23,
  // `null` matches the API response shape for DAGs without version metadata.
  latest_dag_version: null,
  next_dagrun_logical_date: "2024-08-22T00:00:00+00:00",
  next_dagrun_run_after: "2024-08-22T19:00:00+00:00",
  owner_links: {},
  relative_fileloc: "stale_dag.py",
  tags: [],
  timetable_partitioned: false,
  timetable_summary: "* * * * *",
} as unknown as DAGDetailsResponse;

describe("Header", () => {
  it("shows a deactivated badge and hides stale-only next actions for stale dags", () => {
    render(
      <Wrapper>
        <Header dag={mockDag} />
      </Wrapper>,
    );

    expect(screen.getByText(i18n.t("dag:header.status.deactivated"))).toBeInTheDocument();
    expect(screen.queryByText(i18n.t("dag:dagDetails.nextRun"))).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Reparse Dag" })).not.toBeInTheDocument();
  });
});
