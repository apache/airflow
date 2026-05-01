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
import { describe, expect, it } from "vitest";

import { getRunOnLatestVersionState } from "./runOnLatestVersion";

describe("getRunOnLatestVersionState", () => {
  it.each([
    {
      expectedDagVersionsDiffer: true,
      expectedShouldShowRunOnLatestOption: true,
      latestDagVersionNumber: 3,
      name: "shows and defaults on when DAG version numbers differ",
      selectedDagVersionNumber: 2,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestDagVersionNumber: 3,
      name: "does not show when DAG version numbers match and there is no bundle difference",
      selectedDagVersionNumber: 3,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestDagVersionNumber: undefined,
      name: "does not treat a missing latest DAG version number as different",
      selectedDagVersionNumber: 3,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestDagVersionNumber: 3,
      name: "does not treat a missing selected DAG version number as different",
      selectedDagVersionNumber: undefined,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: "bundle-a",
      name: "does not show when task bundle versions match",
      selectedBundleVersion: "bundle-a",
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: true,
      latestBundleVersion: "bundle-b",
      name: "shows when task bundle versions differ",
      selectedBundleVersion: "bundle-a",
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: true,
      latestBundleVersion: null,
      name: "shows when task bundle version differs from a known null latest bundle",
      selectedBundleVersion: "bundle-a",
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: undefined,
      name: "does not compare task bundle version before the latest bundle is loaded",
      selectedBundleVersion: "bundle-a",
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: "bundle-b",
      name: "does not show for task bundle comparison when selected bundle is null",
      selectedBundleVersion: null,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: "bundle-b",
      name: "does not show for task bundle comparison when selected bundle is empty",
      selectedBundleVersion: "",
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: "bundle-b",
      name: "does not show for task bundle comparison when selected bundle is missing",
      selectedBundleVersion: undefined,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: true,
      latestBundleVersion: "bundle-b",
      name: "shows for group fallback when latest bundle is available",
      selectedBundleVersion: undefined,
      useLatestBundleVersionAsFallback: true,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: null,
      name: "does not show for group fallback when latest bundle is null",
      useLatestBundleVersionAsFallback: true,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: "",
      name: "does not show for group fallback when latest bundle is empty",
      useLatestBundleVersionAsFallback: true,
    },
    {
      expectedDagVersionsDiffer: false,
      expectedShouldShowRunOnLatestOption: false,
      latestBundleVersion: undefined,
      name: "does not show for group fallback when latest bundle is missing",
      useLatestBundleVersionAsFallback: true,
    },
  ])(
    "$name",
    ({
      expectedDagVersionsDiffer,
      expectedShouldShowRunOnLatestOption,
      latestBundleVersion,
      latestDagVersionNumber,
      selectedBundleVersion,
      selectedDagVersionNumber,
      useLatestBundleVersionAsFallback,
    }) => {
      expect(
        getRunOnLatestVersionState({
          latestBundleVersion,
          latestDagVersionNumber,
          selectedBundleVersion,
          selectedDagVersionNumber,
          useLatestBundleVersionAsFallback,
        }),
      ).toEqual({
        dagVersionsDiffer: expectedDagVersionsDiffer,
        shouldShowRunOnLatestOption: expectedShouldShowRunOnLatestOption,
      });
    },
  );
});
