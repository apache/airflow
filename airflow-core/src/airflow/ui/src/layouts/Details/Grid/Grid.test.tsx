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
import "@testing-library/jest-dom/vitest";
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { GridRunsResponse } from "openapi/requests/types.gen";

import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { GridSelectionProvider } from "src/context/gridSelection";
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
          <GridSelectionProvider>
            <Grid
              limit={10}
              offset={0}
              onJumpToLatest={vi.fn()}
              setOffset={vi.fn()}
              showVersionIndicatorMode={VersionIndicatorOptions.ALL}
            />
          </GridSelectionProvider>
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

  it("keeps the task name column pinned for the full width of the run columns", () => {
    const { container } = render(
      <Wrapper>
        <GroupsProvider dagId="example_dag">
          <GridSelectionProvider>
            <Grid
              limit={10}
              offset={0}
              onJumpToLatest={vi.fn()}
              setOffset={vi.fn()}
              showVersionIndicatorMode={VersionIndicatorOptions.ALL}
            />
          </GridSelectionProvider>
        </GroupsProvider>
      </Wrapper>,
    );

    // The run columns live in two rows, header then body; each row also holds the sticky task
    // name column. The virtualizer renders no rows without layout, so reach them via the columns.
    const [, bodyRunColumns] = [
      ...new Set([...container.querySelectorAll("[data-run-id]")].map((element) => element.parentElement)),
    ];
    const bodyRow = bodyRunColumns?.parentElement;
    const headerRow = bodyRow?.previousElementSibling;

    // Both rows have to outgrow the scrollport. A row only as wide as the scrollport ends the sticky
    // column's containing block there, so it stops being pinned partway through a horizontal scroll.
    expect(bodyRow).toHaveStyle({ minWidth: "max-content" });
    expect(headerRow).toHaveStyle({ minWidth: "max-content" });
  });

  it("pins the pager buttons to the scrollport, on either side of the run columns", () => {
    // limit below the run count gives an older page, and a non-zero offset a newer one.
    const { container } = render(
      <Wrapper>
        <GroupsProvider dagId="example_dag">
          <GridSelectionProvider>
            <Grid
              limit={1}
              offset={10}
              onJumpToLatest={vi.fn()}
              setOffset={vi.fn()}
              showVersionIndicatorMode={VersionIndicatorOptions.ALL}
            />
          </GridSelectionProvider>
        </GroupsProvider>
      </Wrapper>,
    );

    const [olderButton, newerButton] = [...container.querySelectorAll("button")];
    const olderPager = olderButton?.parentElement;
    const newerPager = newerButton?.parentElement?.parentElement;

    // Sticky to the scrollport rather than absolute to the content, so scrolling cannot carry the
    // buttons out of reach; zero width keeps them out of the flex layout either way.
    for (const pager of [olderPager, newerPager]) {
      expect(pager).toHaveStyle({ position: "sticky", width: "0" });
    }

    // Sticky parks a box at its natural position until the edge pushes it, so each side has to sit
    // on its own side of the columns in the flow.
    const runColumn = container.querySelector("[data-run-id]");

    expect(olderPager?.compareDocumentPosition(runColumn as Node)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(newerPager?.compareDocumentPosition(runColumn as Node)).toBe(Node.DOCUMENT_POSITION_PRECEDING);
  });
});
