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

import { shouldShowRunOnLatestVersionForRun } from "./runOnLatestVersion";

describe("shouldShowRunOnLatestVersionForRun", () => {
  it.each([
    {
      bundleVersion: "git-sha-2",
      expected: true,
      latestDagVersionNumber: 3,
      name: "shows for a version-pinned run whose Dag version is behind the latest",
      onlyNew: false,
      runDagVersionNumber: 2,
    },
    {
      bundleVersion: null,
      expected: false,
      latestDagVersionNumber: 3,
      name: "hides for a non-versioning bundle (null bundle_version) even when Dag versions differ",
      onlyNew: false,
      runDagVersionNumber: 2,
    },
    {
      bundleVersion: "git-sha-3",
      expected: false,
      latestDagVersionNumber: 3,
      name: "hides when the run is already on the latest Dag version",
      onlyNew: false,
      runDagVersionNumber: 3,
    },
    {
      bundleVersion: "git-sha-2",
      expected: false,
      latestDagVersionNumber: 3,
      name: "hides when only queueing new tasks",
      onlyNew: true,
      runDagVersionNumber: 2,
    },
    {
      bundleVersion: "git-sha-2",
      expected: false,
      latestDagVersionNumber: undefined,
      name: "hides when the latest Dag version number is not loaded",
      onlyNew: false,
      runDagVersionNumber: 2,
    },
    {
      bundleVersion: "git-sha-2",
      expected: false,
      latestDagVersionNumber: 3,
      name: "hides when the run's Dag version number is unknown",
      onlyNew: false,
      runDagVersionNumber: undefined,
    },
  ])("$name", ({ bundleVersion, expected, latestDagVersionNumber, onlyNew, runDagVersionNumber }) => {
    expect(
      shouldShowRunOnLatestVersionForRun({
        bundleVersion,
        latestDagVersionNumber,
        onlyNew,
        runDagVersionNumber,
      }),
    ).toBe(expected);
  });
});
