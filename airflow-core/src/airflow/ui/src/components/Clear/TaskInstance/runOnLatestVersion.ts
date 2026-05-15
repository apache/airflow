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
  readonly useLatestBundleVersionAsFallback?: boolean;
};

type RunOnLatestVersionState = {
  readonly dagVersionsDiffer: boolean;
  readonly shouldShowRunOnLatestOption: boolean;
};

const hasBundleVersion = (bundleVersion: string | null | undefined) =>
  bundleVersion !== undefined && bundleVersion !== null && bundleVersion !== "";

export const getRunOnLatestVersionState = ({
  latestBundleVersion,
  latestDagVersionNumber,
  selectedBundleVersion,
  selectedDagVersionNumber,
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
    shouldShowRunOnLatestOption: dagVersionsDiffer || shouldShowForBundleVersion,
  };
};
