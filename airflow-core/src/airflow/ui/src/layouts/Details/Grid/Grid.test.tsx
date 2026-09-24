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
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { GridRunsResponse } from "openapi/requests/types.gen";

import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { GroupsProvider } from "src/context/groups";
import { Wrapper } from "src/utils/Wrapper";

import { Grid } from "./Grid";

const buildRun = (runId: string, versionNumber: number): GridRunsResponse => ({
  dag_id: "example_dag",
  dag_versions: [
    {
      bundle_name: "dags-folder",
      bundle_url: null,
      bundle_version: null,
      created_at: "2026-09-22T00:00:00Z",
      dag_display_name: "example_dag",
      dag_id: "example_dag",
      id: `version-${versionNumber}`,
      version_number: versionNumber,
    },
  ],
  duration: 1,
  end_date: null,
  has_missed_deadline: false,
  has_note: false,
  queued_at: null,
  run_after: "2026-09-22T00:00:00Z",
  run_id: runId,
  run_type: "manual",
  start_date: null,
  state: "success",
});

const gridRuns = [buildRun("run_v2", 2), buildRun("run_v1", 1)];

vi.mock("src/queries/useGridRuns.ts", () => ({
  useGridRuns: () => ({ data: gridRuns, isLoading: false }),
}));

vi.mock("src/queries/useGridStructure.ts", () => ({
  useGridStructure: () => ({ data: [{ id: "say_hello", label: "say_hello" }] }),
}));

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: () => ({ summariesByRunId: new Map() }),
}));

describe("Grid", () => {
  it("keeps run columns below the sticky task name column during horizontal scroll", () => {
    const { container } = render(
      <Wrapper>
        <GroupsProvider dagId="example_dag">
          <Grid
            limit={10}
            offset={0}
            onJumpToLatest={vi.fn()}
            setOffset={vi.fn()}
            showVersionIndicatorMode={VersionIndicatorOptions.ALL}
          />
        </GroupsProvider>
      </Wrapper>,
    );

    const runContainers = new Set(
      [...container.querySelectorAll("[data-run-id]")].map((element) => element.parentElement),
    );

    expect(runContainers.size).toBe(2);
    for (const runContainer of runContainers) {
      expect(runContainer).toHaveStyle({ isolation: "isolate" });
    }
  });
});
