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

import type { DagVersionDiffResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { VersionDiff } from "./VersionDiff";

const redacted: DagVersionDiffResponse = {
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
  target_version_number: 2,
  total_changes: 3,
  truncated: false,
  values_status: "unavailable",
};

const authorized: DagVersionDiffResponse = {
  ...redacted,
  changes: [
    {
      after_digest: "sha256:beef",
      after_value: "bob",
      before_digest: "sha256:cafe",
      before_value: "alice",
      category: "metadata",
      impact: "metadata",
      occurrence_count: 1,
      operation: "changed",
      path: "/dag/tasks/extract/owner",
    },
  ],
  total_changes: 1,
  values_status: "available",
};

const renderDiff = (diff: DagVersionDiffResponse) =>
  render(
    <Wrapper>
      <VersionDiff baseVersionNumber={1} diff={diff} targetVersionNumber={2} />
    </Wrapper>,
  );

// The test harness passes translation keys through untranslated, so assertions on chrome use the
// key and assertions on content use the data the API returned.
describe("VersionDiff", () => {
  it("withholds value columns when the caller may not see values", () => {
    renderDiff(redacted);

    expect(screen.getByText("/dag/tasks/*/retries")).toBeInTheDocument();
    expect(screen.queryByText("versions.columns.before")).not.toBeInTheDocument();
    expect(screen.queryByText("versions.columns.after")).not.toBeInTheDocument();
    // Says what would grant them, since nothing here can.
    expect(screen.getByText("versions.codeAccess")).toBeInTheDocument();
  });

  it("shows values and the identifying path once the caller may see them", () => {
    renderDiff(authorized);

    expect(screen.getByText("/dag/tasks/extract/owner")).toBeInTheDocument();
    expect(screen.getByText("versions.columns.before")).toBeInTheDocument();
    expect(screen.getByText("versions.columns.after")).toBeInTheDocument();
    expect(screen.getByText("alice")).toBeInTheDocument();
    expect(screen.getByText("bob")).toBeInTheDocument();
  });

  it("warns that a truncated count is a lower bound", () => {
    renderDiff({ ...redacted, truncated: true });

    expect(screen.getByText("versions.truncated.description")).toBeInTheDocument();
  });

  it("truncates a value too large to read in a table cell", () => {
    const wholeTask = { __type: "operator", __var: { task_id: "extract", template_fields: "x".repeat(400) } };

    renderDiff({
      ...authorized,
      changes: [
        {
          after_value: wholeTask,
          category: "task",
          impact: "execution",
          occurrence_count: 1,
          operation: "added",
          path: "/dag/tasks/extract",
        },
      ],
    });

    const text = screen.getByText(/^\{"__type"/u).textContent;

    expect(text.endsWith("…")).toBe(true);
    expect(text.length).toBeLessThan(200);
  });

  it("reports why a comparison could not be made instead of an empty table", () => {
    renderDiff({
      ...redacted,
      changes: [],
      mode: "unavailable",
      total_changes: 0,
      unavailable_reason: "serialized_dag_missing",
    });

    expect(screen.getByText("serialized_dag_missing")).toBeInTheDocument();
    expect(screen.queryByText("versions.columns.path")).not.toBeInTheDocument();
  });
});
