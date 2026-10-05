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
import { describe, expect, it, vi } from "vitest";

import type { DagVersionResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { VersionCompareSelect } from "./VersionCompareSelect";

const { mockGetDagVersions } = vi.hoisted(() => ({ mockGetDagVersions: vi.fn() }));

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return { ...actual, useParams: () => ({ dagId: "test_dag" }) };
});

vi.mock("openapi/queries", () => ({ useDagVersionServiceGetDagVersions: mockGetDagVersions }));

const versions: Array<DagVersionResponse> = [3, 2, 1].map((versionNumber) => ({
  bundle_name: "dags-folder",
  bundle_url: null,
  bundle_version: null,
  created_at: "2025-01-01T00:00:00Z",
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  id: `00000000-0000-0000-0000-00000000000${versionNumber}`,
  version_number: versionNumber,
}));

describe("VersionCompareSelect", () => {
  it("asks for versions newest first, since the list is read top-down", () => {
    mockGetDagVersions.mockReturnValue({ data: { dag_versions: versions, total_entries: 3 } });

    render(<VersionCompareSelect label="Compare from" onVersionChange={vi.fn()} />, { wrapper: Wrapper });

    expect(mockGetDagVersions).toHaveBeenCalledWith({ dagId: "test_dag", orderBy: ["-version_number"] });
  });

  it("prompts with a translated placeholder when the caller supplies none", () => {
    mockGetDagVersions.mockReturnValue({ data: { dag_versions: versions, total_entries: 3 } });

    render(<VersionCompareSelect label="Compare from" onVersionChange={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByText("versionSelect.placeholder")).toBeInTheDocument();
  });

  it("prefers the placeholder the caller supplies", () => {
    mockGetDagVersions.mockReturnValue({ data: { dag_versions: versions, total_entries: 3 } });

    render(
      <VersionCompareSelect
        label="Compare from"
        onVersionChange={vi.fn()}
        placeholder="Pick a version to compare"
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("Pick a version to compare")).toBeInTheDocument();
    expect(screen.queryByText("versionSelect.placeholder")).not.toBeInTheDocument();
  });
});
