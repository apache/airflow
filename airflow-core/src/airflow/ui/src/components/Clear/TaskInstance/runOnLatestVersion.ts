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

type RunOnLatestVersionParams = {
  readonly latestBundleVersion?: string | null;
  readonly latestDagVersionNumber?: number | null;
  readonly selectedBundleVersion?: string | null;
  readonly selectedDagVersionNumber?: number | null;
  /**
   * True when the *run* being cleared has no Dag version at all, which is the case for
   * anything carried over from Airflow 2. There is nothing to re-run it on but the latest
   * version, so the backend forces that regardless of the request. Keep this keyed off the
   * run: a task instance with no version of its own is given its run's version, not the
   * latest, so deriving this from the task instance would promise the wrong thing.
   */
  readonly selectedVersionMissing?: boolean;
  readonly useLatestBundleVersionAsFallback?: boolean;
};

type RunOnLatestVersionState = {
  readonly dagVersionsDiffer: boolean;
  /**
   * Drives how the checkbox renders, not what is submitted. A clear can span several runs
   * (via past/future) while the request carries one flag for all of them, so forcing it
   * would pin runs the user never selected. The backend forces each version-less run on
   * its own instead.
   */
  readonly runOnLatestVersionForced: boolean;
  readonly shouldShowRunOnLatestOption: boolean;
};

const hasBundleVersion = (bundleVersion: string | null | undefined) =>
  bundleVersion !== undefined && bundleVersion !== null && bundleVersion !== "";

export const getRunOnLatestVersionState = ({
  latestBundleVersion,
  latestDagVersionNumber,
  selectedBundleVersion,
  selectedDagVersionNumber,
  selectedVersionMissing = false,
  useLatestBundleVersionAsFallback = false,
}: RunOnLatestVersionParams): RunOnLatestVersionState => {
  const dagVersionsDiffer =
    latestDagVersionNumber !== undefined &&
    latestDagVersionNumber !== null &&
    selectedDagVersionNumber !== undefined &&
    selectedDagVersionNumber !== null &&
    latestDagVersionNumber !== selectedDagVersionNumber;

  const shouldShowForBundleVersion = useLatestBundleVersionAsFallback
    ? hasBundleVersion(latestBundleVersion)
    : latestBundleVersion !== undefined &&
      hasBundleVersion(selectedBundleVersion) &&
      latestBundleVersion !== selectedBundleVersion;

  return {
    dagVersionsDiffer,
    runOnLatestVersionForced: selectedVersionMissing,
    shouldShowRunOnLatestOption:
      selectedVersionMissing ||
      (dagVersionsDiffer && hasBundleVersion(latestBundleVersion)) ||
      shouldShowForBundleVersion,
  };
};
