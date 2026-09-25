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
/*
 * Separate from VersionDiff.test.tsx: importing the i18n config initializes the shared instance for
 * the whole file, and the sibling asserts on untranslated keys.
 */
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";

import type { DagVersionDiffResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../../../public/i18n/locales/en/dag.json";
import { VersionDiff } from "./VersionDiff";

const oneChange: DagVersionDiffResponse = {
  base_version_number: 1,
  changes: [
    {
      after_value: "bob",
      before_value: "alice",
      category: "metadata",
      impact: "authorization",
      occurrence_count: 1,
      operation: "changed",
      path: "/dag/tasks/extract/owner",
    },
  ],
  diff_schema_version: 1,
  mode: "observed_state",
  serializer_versions: { base: 3, target: 3 },
  target_version_number: 2,
  total_changes: 1,
  truncated: false,
  values_status: "available",
};

describe("VersionDiff in English", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
  });

  it("labels the enum values the API returns", () => {
    render(
      <Wrapper>
        <VersionDiff baseVersionNumber={1} diff={oneChange} targetVersionNumber={2} />
      </Wrapper>,
    );

    expect(screen.getByText("Changed")).toBeInTheDocument();
    expect(screen.getByText("Metadata")).toBeInTheDocument();
    expect(screen.getByText("Authorization")).toBeInTheDocument();
  });

  it("counts a single change in the singular", () => {
    render(
      <Wrapper>
        <VersionDiff baseVersionNumber={1} diff={oneChange} targetVersionNumber={2} />
      </Wrapper>,
    );

    expect(screen.getByText(/^1 change ·/u)).toBeInTheDocument();
  });
});
