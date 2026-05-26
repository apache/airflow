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

import { Wrapper } from "src/utils/Wrapper";

import { DagVersionSelect } from "./DagVersionSelect";

const dagVersionV1 = {
  bundle_name: "dags-folder",
  bundle_version: null,
  created_at: "2025-01-01T00:00:00Z",
  dag_id: "test_dag",
  version_number: 1,
};
const dagVersionV2 = {
  bundle_name: "dags-folder",
  bundle_version: null,
  created_at: "2025-01-02T00:00:00Z",
  dag_id: "test_dag",
  version_number: 2,
};
const dagVersionV3 = {
  bundle_name: "dags-folder",
  bundle_version: null,
  created_at: "2025-01-03T00:00:00Z",
  dag_id: "test_dag",
  version_number: 3,
};

const allVersions = [dagVersionV3, dagVersionV2, dagVersionV1];

let mockParams: Record<string, string> = { dagId: "test_dag" };

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => mockParams,
  };
});

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: vi.fn(() => ({
    data: undefined,
    isLoading: false,
  })),
  useDagVersionServiceGetDagVersions: vi.fn(() => ({
    data: { dag_versions: allVersions, total_entries: 3 },
    isLoading: false,
  })),
}));

vi.mock("src/hooks/useSelectedVersion", () => ({
  default: vi.fn(() => undefined),
}));

const { useDagRunServiceGetDagRun } = await import("openapi/queries");

const mockRunData = {
  bundle_version: null,
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_versions: [dagVersionV1, dagVersionV2],
  end_date: null,
  has_missed_deadline: false,
  logical_date: null,
  note: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_id: "run_1",
  run_type: "manual" as const,
  start_date: null,
  state: "success" as const,
  triggered_by: "ui" as const,
  triggering_user_name: null,
};

const getItems = (container: HTMLElement) => container.querySelectorAll(".chakra-select__item");

describe("DagVersionSelect", () => {
  it("shows all versions when no DagRun is selected", () => {
    mockParams = { dagId: "test_dag" };
    const { container } = render(<DagVersionSelect />, { wrapper: Wrapper });

    expect(getItems(container)).toHaveLength(3);
  });

  it("shows only the selected run's versions when a DagRun is selected", () => {
    mockParams = { dagId: "test_dag", runId: "run_1" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: mockRunData,
      isLoading: false,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    const { container } = render(<DagVersionSelect />, { wrapper: Wrapper });

    expect(getItems(container)).toHaveLength(2);
  });
});
