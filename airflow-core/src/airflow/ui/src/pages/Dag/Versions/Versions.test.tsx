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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { DagVersionDiffResponse, DagVersionResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Versions } from "./Versions";

const { mockGetDagVersionDiff, mockGetDagVersions } = vi.hoisted(() => ({
  mockGetDagVersionDiff: vi.fn(),
  mockGetDagVersions: vi.fn(),
}));

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => ({ dagId: "test_dag" }),
  };
});

vi.mock("openapi/queries", () => ({
  useDagVersionServiceGetDagVersionDiff: mockGetDagVersionDiff,
  useDagVersionServiceGetDagVersions: mockGetDagVersions,
}));

const diff: DagVersionDiffResponse = {
  base_version_number: 1,
  changes: [
    {
      category: "task",
      impact: "execution",
      occurrence_count: 3,
      operation: "changed",
      path: "/dag/tasks/*/retries",
    },
  ],
  diff_schema_version: 1,
  mode: "observed_state",
  serialized_dag_schema_versions: { base: 3, target: 3 },
  target_version_number: 3,
  total_changes: 3,
  truncated: false,
  values_status: "unavailable",
};

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

const SearchProbe = () => <div data-testid="search">{useLocation().search}</div>;

const lastDiffRequest = () =>
  mockGetDagVersionDiff.mock.calls.at(-1)?.[0] as Record<string, unknown> | undefined;

const lastDiffOptions = () =>
  mockGetDagVersionDiff.mock.calls.at(-1)?.[2] as Record<string, unknown> | undefined;

// Chakra compiles `visibility` to a class, so the two states are compared rather than read.
const progressClassOf = (view: { container: HTMLElement }) =>
  view.container.querySelector(".chakra-progress__root")?.className;

const renderVersions = (search: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/test_dag/versions${search}`]}>
        <TimezoneProvider>
          <Versions />
          <SearchProbe />
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

// The test harness passes translation keys through untranslated, so assertions on chrome use the key.
describe("Versions", () => {
  beforeEach(() => {
    mockGetDagVersionDiff.mockReturnValue({ data: diff, error: null, isLoading: false });
    mockGetDagVersions.mockReturnValue({
      data: { dag_versions: versions, total_entries: 3 },
      isLoading: false,
    });
  });

  it("compares the versions named in the URL", () => {
    renderVersions("?base_version_number=1&target_version_number=3");

    expect(lastDiffRequest()).toEqual({
      baseVersionNumber: 1,
      dagId: "test_dag",
      maxChanges: undefined,
      targetVersionNumber: 3,
    });
    // Without it the table blanks out whenever the bound changes the query key.
    expect(typeof lastDiffOptions()?.placeholderData).toBe("function");
    expect(screen.getByText("/dag/tasks/*/retries")).toBeInTheDocument();
  });

  it.each(["abc", "1.5", "0", "-1", "Infinity", "", "1e21"])(
    "requests nothing while a version parameter is %s",
    (base) => {
      renderVersions(`?base_version_number=${base}&target_version_number=3`);

      expect(mockGetDagVersionDiff).not.toHaveBeenCalled();
      expect(screen.getByText("versions.selectPrompt")).toBeInTheDocument();
    },
  );

  it("puts a chosen version in the URL", async () => {
    renderVersions("");

    // The listbox stays aria-hidden until it animates open, so the options are matched directly.
    fireEvent.click(screen.getAllByRole("combobox")[0] as HTMLElement);
    fireEvent.click(screen.getAllByRole("option", { hidden: true })[0] as HTMLElement);

    await waitFor(() => expect(screen.getByTestId("search")).toHaveTextContent("base_version_number=3"));
  });

  it("prompts for a version with translated text", () => {
    renderVersions("");

    expect(screen.getAllByText("versionSelect.placeholder")).toHaveLength(2);
  });

  it("forwards max changes from the URL", () => {
    renderVersions("?base_version_number=1&target_version_number=3&max_changes=42");

    expect(lastDiffRequest()).toMatchObject({ maxChanges: 42 });
    expect(screen.getByLabelText("versions.maxChanges")).toHaveValue("42");
  });

  it("clamps a max changes the endpoint would reject", () => {
    renderVersions("?base_version_number=1&target_version_number=3&max_changes=99999");

    expect(lastDiffRequest()).toMatchObject({ maxChanges: 5000 });
    expect(screen.getByLabelText("versions.maxChanges")).toHaveValue("5000");
  });

  it.each([
    ["42", "max_changes=42"],
    ["99999", "max_changes=5000"],
  ])("writes a typed max changes of %s to the URL as %s", async (typed, expected) => {
    renderVersions("?base_version_number=1&target_version_number=3");

    const input = screen.getByLabelText("versions.maxChanges");

    fireEvent.focus(input);
    fireEvent.input(input, { target: { value: typed } });

    await waitFor(() => expect(screen.getByTestId("search")).toHaveTextContent(expected));
  });

  it.each(["", "0"])("drops the max changes parameter when the field becomes %s", async (typed) => {
    renderVersions("?base_version_number=1&target_version_number=3&max_changes=42");

    const input = screen.getByLabelText("versions.maxChanges");

    fireEvent.focus(input);
    fireEvent.input(input, { target: { value: typed } });

    await waitFor(() => expect(screen.getByTestId("search")).not.toHaveTextContent("max_changes"));
  });

  it("reports a failed comparison", () => {
    mockGetDagVersionDiff.mockReturnValue({
      data: undefined,
      error: { body: { detail: "boom" }, status: 400 },
      isLoading: false,
    });

    renderVersions("?base_version_number=1&target_version_number=3");

    expect(screen.getByText("boom")).toBeInTheDocument();
  });

  it("shows progress only while the comparison loads", () => {
    mockGetDagVersionDiff.mockReturnValue({ data: undefined, error: null, isLoading: true });
    const loading = renderVersions("?base_version_number=1&target_version_number=3");

    mockGetDagVersionDiff.mockReturnValue({ data: diff, error: null, isLoading: false });
    const loaded = renderVersions("?base_version_number=1&target_version_number=3");

    expect(progressClassOf(loading)).not.toBe(progressClassOf(loaded));
    expect(progressClassOf(loading)).toBeDefined();
  });

  it("puts a typed max changes in the URL", async () => {
    renderVersions("?base_version_number=1&target_version_number=3");

    const input = screen.getByLabelText("versions.maxChanges");

    fireEvent.focus(input);
    fireEvent.input(input, { target: { value: "42" } });

    await waitFor(() => expect(screen.getByTestId("search")).toHaveTextContent("max_changes=42"));
  });
});
