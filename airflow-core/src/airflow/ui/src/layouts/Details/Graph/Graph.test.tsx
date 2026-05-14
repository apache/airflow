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
import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { Wrapper } from "src/utils/Wrapper";

import { Graph } from "./Graph";

// testsSetup.ts globally mocks Graph to null so full-page tests don't need to
// stub ELK layout data. Unmock it here so we test the real component.
vi.unmock("src/layouts/Details/Graph/Graph");

let mockParams: Record<string, string> = { dagId: "test_dag" };

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [new URLSearchParams()],
  };
});

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined, isLoading: false })),
  useStructureServiceStructureData: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/queries/useDependencyGraph", () => ({
  useDependencyGraph: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/components/Graph/useGraphLayout", () => ({
  useGraphLayout: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: vi.fn(() => ({ summariesByRunId: new Map() })),
}));

vi.mock("src/hooks/useSelectedVersion", () => ({
  default: vi.fn(() => 1),
}));

vi.mock("src/context/openGroups", () => ({
  useOpenGroups: vi.fn(() => ({ allGroupIds: [], openGroupIds: [], setAllGroupIds: vi.fn() })),
}));

vi.mock("@xyflow/react", async () => {
  const actual = await vi.importActual("@xyflow/react");

  return {
    ...actual,
    ReactFlow: vi.fn(() => null),
    useReactFlow: vi.fn(() => ({ fitView: vi.fn(), getZoom: vi.fn() })),
  };
});

const mockDagRun = {
  bundle_version: null,
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_versions: [],
  end_date: null,
  has_missed_deadline: false,
  logical_date: null,
  note: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_id: "run_1",
  run_type: "manual" as const,
  start_date: "2025-01-01T00:00:01Z",
  state: "running" as const,
  triggered_by: "ui" as const,
  triggering_user_name: null,
};

describe("Graph", () => {
  it("passes states to useGridTiSummariesStream when a runId is present", () => {
    mockParams = { dagId: "test_dag", runId: "run_1" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: mockDagRun,
      isLoading: false,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: ["run_1"],
        states: ["running"],
      }),
    );
  });

  it("passes undefined states while dag run is still loading", () => {
    mockParams = { dagId: "test_dag", runId: "run_1" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      isLoading: true,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: ["run_1"],
        states: undefined,
      }),
    );
  });

  it("passes undefined states to useGridTiSummariesStream when there is no runId", () => {
    mockParams = { dagId: "test_dag" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      isLoading: false,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: [],
        states: undefined,
      }),
    );
  });
});
