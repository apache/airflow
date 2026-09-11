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
import { describe, expect, it } from "vitest";

import type { DagRunAssetReference } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { TriggeredRuns } from "./TriggeredRuns";

const makeRun = (overrides: Partial<DagRunAssetReference>): DagRunAssetReference =>
  ({
    dag_id: "dag_1",
    data_interval_end: null,
    data_interval_start: null,
    end_date: null,
    logical_date: null,
    partition_key: null,
    run_id: "run_1",
    start_date: "2025-01-01T00:00:00Z",
    state: "success",
    triggering: true,
    ...overrides,
  }) satisfies DagRunAssetReference;

describe("TriggeredRuns", () => {
  it("labels a triggering run as triggered", () => {
    render(<TriggeredRuns dagRuns={[makeRun({ triggering: true })]} />, { wrapper: Wrapper });

    expect(screen.getByText(/triggered dagRun_one/u)).toBeInTheDocument();
    expect(screen.queryByText(/includedIn/u)).not.toBeInTheDocument();
  });

  it("labels a non-triggering run as included", () => {
    render(<TriggeredRuns dagRuns={[makeRun({ triggering: false })]} />, { wrapper: Wrapper });

    expect(screen.getByText(/includedIn dagRun_one/u)).toBeInTheDocument();
    expect(screen.queryByText(/triggered dagRun/u)).not.toBeInTheDocument();
  });

  it("splits a mix of triggering and included runs into separate labels", () => {
    render(
      <TriggeredRuns
        dagRuns={[
          makeRun({ dag_id: "dag_triggered", run_id: "r1", triggering: true }),
          makeRun({ dag_id: "dag_included", run_id: "r2", triggering: false }),
        ]}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/triggered dagRun_one/u)).toBeInTheDocument();
    expect(screen.getByText(/includedIn dagRun_one/u)).toBeInTheDocument();
  });

  it("groups multiple included runs behind a single count label", () => {
    render(
      <TriggeredRuns
        dagRuns={[
          makeRun({ dag_id: "dag_a", run_id: "r1", triggering: false }),
          makeRun({ dag_id: "dag_b", run_id: "r2", triggering: false }),
        ]}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/2 includedIn dagRun_other/u)).toBeInTheDocument();
  });
});
