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
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  $DagVersionDiffCategory,
  $DagVersionDiffImpact,
  $DagVersionDiffOperation,
} from "openapi/requests/schemas.gen";
import type { DagVersionDiffChangeResponse, DagVersionDiffResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../../../public/i18n/locales/en/dag.json";
import { VersionDiff } from "./VersionDiff";

// The cells render these tokens through the label maps below, so a value the engine gains without
// a label would reach the user as a bare token.
const LABEL_GROUPS = {
  categories: dagLocale.versions.categories,
  impacts: dagLocale.versions.impacts,
  operations: dagLocale.versions.operations,
};

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
  serializer_versions: { base: 3, target: 3 },
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
describe.each([
  ["categories", $DagVersionDiffCategory.enum],
  ["impacts", $DagVersionDiffImpact.enum],
  ["operations", $DagVersionDiffOperation.enum],
] as const)("VersionDiff %s labels", (group, values) => {
  it("cover exactly what the API can return", () => {
    expect(Object.keys(LABEL_GROUPS[group]).sort()).toEqual([...values].sort());
  });
});

describe("VersionDiff summary", () => {
  it("carries both plural forms, since i18next resolves it with a count", () => {
    // A single `summary` key would render "1 changes" for a one-change comparison.
    expect(dagLocale.versions).not.toHaveProperty("summary");
    expect(dagLocale.versions.summary_one).toContain("{{count, number}}");
    expect(dagLocale.versions.summary_other).toContain("{{count, number}}");
  });
});

describe("VersionDiff", () => {
  it("withholds value columns when the caller may not see values", () => {
    renderDiff(redacted);

    expect(screen.getByText("/dag/tasks/*/retries")).toBeInTheDocument();
    expect(screen.queryByText("versions.columns.before")).not.toBeInTheDocument();
    expect(screen.queryByText("versions.columns.after")).not.toBeInTheDocument();
  });

  it("renders every returned record", () => {
    // The endpoint bounds the payload itself, so the table must not page on top of that and hide
    // records the caller explicitly asked for.
    const many: Array<DagVersionDiffChangeResponse> = Array.from({ length: 24 }, (_, index) => ({
      category: "task",
      impact: "execution",
      occurrence_count: 1,
      operation: "changed",
      path: `/dag/tasks/task_${index}/retries`,
    }));

    renderDiff({ ...redacted, changes: many, total_changes: many.length });

    for (const change of many) {
      expect(screen.getByText(change.path)).toBeInTheDocument();
    }
  });

  it("keeps the comparison's boundary reachable from above the table", () => {
    renderDiff(redacted);

    // Below the table it would be pushed off-screen by exactly the long diffs that need it.
    expect(screen.queryByText("versions.observedStateBoundary")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "versions.aboutThisComparison" })).toBeInTheDocument();
  });

  it("shows values and the identifying path once the caller may see them", () => {
    renderDiff(authorized);

    expect(screen.getByText("/dag/tasks/extract/owner")).toBeInTheDocument();
    expect(screen.getByText("versions.columns.before")).toBeInTheDocument();
    expect(screen.getByText("versions.columns.after")).toBeInTheDocument();
    expect(screen.getByText("alice")).toBeInTheDocument();
    expect(screen.getByText("bob")).toBeInTheDocument();
  });

  it("says a truncated count is a lower bound", () => {
    renderDiff({ ...redacted, truncated: true });

    expect(screen.getByText("versions.summaryAtLeast")).toBeInTheDocument();
    // What the bound was, and how to lift it, belongs on the field that sets it.
    expect(screen.queryByText("versions.limitReached.title")).not.toBeInTheDocument();
  });

  it("counts changes once, in the summary, not again above the table", () => {
    renderDiff(redacted);

    expect(screen.getByText("versions.summary")).toBeInTheDocument();
    expect(screen.queryByText(/dag:versions\.record|versions\.record/u)).not.toBeInTheDocument();
  });

  it("gives an exact count when nothing was dropped", () => {
    renderDiff(redacted);

    expect(screen.getByText("versions.summary")).toBeInTheDocument();
    expect(screen.queryByText("versions.limitReached.title")).not.toBeInTheDocument();
  });

  it("leaves the side an added change does not have blank", () => {
    renderDiff({
      ...authorized,
      changes: [
        {
          after_digest: "sha256:beef",
          after_value: "extract",
          before_digest: null,
          category: "task",
          impact: "execution",
          occurrence_count: 1,
          operation: "added",
          path: "/dag/tasks/extract",
        },
      ],
    });

    const cells = within(screen.getAllByRole("row")[1] as HTMLElement).getAllByRole("cell");

    // A dash would read as a value the base version holds, rather than as a side that is absent.
    expect(cells[5]).toBeEmptyDOMElement();
    expect(screen.queryByText("—")).not.toBeInTheDocument();
  });

  it("previews a structured value as JSON rather than flattening it into the cell", () => {
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

    expect(screen.getByTestId("json-preview-badges")).toBeInTheDocument();
    expect(screen.getByText("__type:")).toBeInTheDocument();
    // The serialized payload itself stays in the editor the preview opens.
    expect(screen.queryByText(/^\{"__type"/u)).not.toBeInTheDocument();
  });

  it("shows an empty structure as a value, since it has no entries to preview", () => {
    renderDiff({
      ...authorized,
      changes: [
        {
          after_value: [],
          before_value: {},
          category: "task",
          impact: "execution",
          occurrence_count: 1,
          operation: "changed",
          path: "/dag/tasks/extract/outlets",
        },
      ],
    });

    expect(screen.getByText("{}")).toBeInTheDocument();
    expect(screen.getByText("[]")).toBeInTheDocument();
  });

  it("truncates a scalar too long to read in a table cell", () => {
    renderDiff({
      ...authorized,
      changes: [
        {
          after_value: `s3://bucket/${"x".repeat(400)}`,
          category: "task",
          impact: "execution",
          occurrence_count: 1,
          operation: "changed",
          path: "/dag/tasks/extract/bash_command",
        },
      ],
    });

    const text = screen.getByText(/^s3:\/\/bucket\//u).textContent;

    expect(text.endsWith("…")).toBe(true);
    expect(text.length).toBeLessThan(200);
  });

  it("renders a stored null as a value, not as an absent side", () => {
    renderDiff({
      ...authorized,
      changes: [
        {
          after_digest: "sha256:beef",
          after_value: null,
          before_digest: "sha256:cafe",
          before_value: "alice",
          category: "task",
          impact: "execution",
          occurrence_count: 1,
          operation: "changed",
          path: "/dag/tasks/extract/owner",
        },
      ],
    });

    expect(screen.getByText("null")).toBeInTheDocument();
    expect(screen.queryByText("—")).not.toBeInTheDocument();
  });

  it("puts each side in its own column", () => {
    renderDiff(authorized);

    const cells = within(screen.getAllByRole("row")[1] as HTMLElement).getAllByRole("cell");

    expect(cells[4]).toHaveTextContent("1");
    expect(cells[5]).toHaveTextContent("alice");
    expect(cells[6]).toHaveTextContent("bob");
  });

  it("shows no disclosure chrome when the caller may see values", () => {
    renderDiff(authorized);

    expect(screen.queryByText("versions.valuesHidden")).not.toBeInTheDocument();
    expect(screen.queryByText("versions.codeAccess")).not.toBeInTheDocument();
  });

  it("flags hidden values, saying on hover what would grant them", async () => {
    vi.useFakeTimers();

    try {
      renderDiff(redacted);

      const pill = screen.getByText("versions.valuesHidden");

      // Only on hover: not repeated as inline text next to the pill.
      expect(screen.queryByText("versions.codeAccess")).not.toBeInTheDocument();

      await act(async () => {
        fireEvent.focus(pill);
        fireEvent.pointerEnter(pill);
        await vi.advanceTimersByTimeAsync(500);
      });

      expect(screen.getByRole("tooltip")).toHaveTextContent("versions.codeAccess");
    } finally {
      vi.useRealTimers();
    }
  });

  it("says so when two versions hold the same state", () => {
    renderDiff({ ...redacted, changes: [], total_changes: 0 });

    expect(screen.getByText("versions.noChanges")).toBeInTheDocument();
  });

  it("reports an unavailable comparison that carries no reason", () => {
    renderDiff({
      ...redacted,
      changes: [],
      mode: "unavailable",
      total_changes: 0,
      unavailable_reason: null,
    });

    expect(screen.getByText("versions.unavailable.withoutReason")).toBeInTheDocument();
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
