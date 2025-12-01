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
import { useQueries } from "@tanstack/react-query";
import { useMemo } from "react";

import { UseDagRunServiceGetDagRunKeyFn } from "openapi/queries";
import type { GridRunsResponse } from "openapi/requests";
import { DagRunService } from "openapi/requests/services.gen";
import type { VersionIndicatorDisplayOption } from "src/constants/showVersionIndicatorOptions";
import { VersionIndicatorDisplayOptions } from "src/constants/showVersionIndicatorOptions";

export type GridRunWithVersionFlags = {
  bundleVersion?: string;
  dagVersionNumber?: number;
  hasMixedVersions: boolean;
  isBundleVersionChange: boolean;
  isDagVersionChange: boolean;
} & GridRunsResponse;

type UseGridRunsWithVersionFlagsParams = {
  dagId: string;
  gridRuns: Array<GridRunsResponse> | undefined;
  showVersionIndicatorMode?: VersionIndicatorDisplayOption;
};

// Hook to fetch version information and calculate version change flags for grid runs.
// Skips API calls when version indicators are disabled.
export const useGridRunsWithVersionFlags = ({
  dagId,
  gridRuns,
  showVersionIndicatorMode,
}: UseGridRunsWithVersionFlagsParams): Array<GridRunWithVersionFlags> | undefined => {
  // Skip API calls when version indicators are disabled
  const isVersionIndicatorEnabled = showVersionIndicatorMode !== VersionIndicatorDisplayOptions.NONE;

  const dagRunQueries = useQueries({
    queries: (gridRuns ?? []).map((run) => ({
      enabled: isVersionIndicatorEnabled && Boolean(dagId) && Boolean(run.run_id),
      queryFn: () => DagRunService.getDagRun({ dagId, dagRunId: run.run_id }),
      queryKey: UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId: run.run_id }),
    })),
  });

  return useMemo(() => {
    if (!gridRuns) {
      return undefined;
    }

    // When NONE mode, skip all version calculations and API calls
    if (!isVersionIndicatorEnabled) {
      return gridRuns.map((run) => ({
        ...run,
        bundleVersion: undefined,
        dagVersionNumber: undefined,
        hasMixedVersions: false,
        isBundleVersionChange: false,
        isDagVersionChange: false,
      }));
    }

    return gridRuns.map((run, index) => {
      const currentDagRunData = dagRunQueries[index]?.data;
      const prevDagRunData = dagRunQueries[index + 1]?.data;

      // Get Dag versions info
      const currentDagVersions = currentDagRunData?.dag_versions;
      const prevDagVersions = prevDagRunData?.dag_versions;
      const currentDagVersionNumber = currentDagVersions?.at(-1)?.version_number;
      const prevDagVersionNumber = prevDagVersions?.at(-1)?.version_number;
      const hasMixedVersions = (currentDagVersions?.length ?? 0) > 1;

      // Get Bundle version info
      const currentBundleVersion = currentDagRunData?.bundle_version ?? undefined;
      const prevBundleVersion = prevDagRunData?.bundle_version ?? undefined;

      return {
        ...run,
        bundleVersion: currentBundleVersion,
        dagVersionNumber: currentDagVersionNumber,
        hasMixedVersions,
        isBundleVersionChange: Boolean(
          prevDagRunData &&
            currentBundleVersion !== undefined &&
            prevBundleVersion !== undefined &&
            currentBundleVersion !== prevBundleVersion,
        ),
        isDagVersionChange: Boolean(
          prevDagRunData &&
            currentDagVersionNumber !== undefined &&
            prevDagVersionNumber !== undefined &&
            currentDagVersionNumber !== prevDagVersionNumber,
        ),
      };
    });
  }, [gridRuns, dagRunQueries, isVersionIndicatorEnabled]);
};
