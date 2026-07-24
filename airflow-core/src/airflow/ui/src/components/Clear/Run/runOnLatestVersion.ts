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

type RunOnLatestVersionForRunParams = {
  readonly bundleVersion: string | null;
  readonly latestDagVersionNumber?: number;
  readonly onlyNew: boolean;
  readonly runDagVersionNumber?: number;
};

/**
 * Whether to show the "run with latest bundle version" option when clearing a Dag run.
 *
 * It only has an effect when the run is pinned to a specific bundle version (versioning-capable
 * bundles such as GitDagBundle). Non-versioning bundles like LocalDagBundle have a null
 * bundle_version and always resolve to the latest serialized Dag, so the option is hidden there
 * rather than offering a choice that does nothing.
 */
export const shouldShowRunOnLatestVersionForRun = ({
  bundleVersion,
  latestDagVersionNumber,
  onlyNew,
  runDagVersionNumber,
}: RunOnLatestVersionForRunParams): boolean => {
  const versionsDiffer =
    latestDagVersionNumber !== undefined &&
    runDagVersionNumber !== undefined &&
    latestDagVersionNumber !== runDagVersionNumber;

  return versionsDiffer && !onlyNew && bundleVersion !== null;
};
