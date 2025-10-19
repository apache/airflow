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
import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { useParams } from "react-router-dom";

import { GridService } from "openapi/requests";
import type { GridRunsResponse, GridTISummaries } from "openapi/requests";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useGridTISummariesBatch = ({
  enabled,
  runs,
}: {
  enabled?: boolean;
  runs: Array<GridRunsResponse>;
}) => {
  const { dagId = "" } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });
  const runIds = runs.map((run) => run.run_id);

  // Check if any run has a pending state
  const hasPendingState = runs.some((run) => isStatePending(run.state));

  const { data: batchSummaries, ...rest } = useQuery({
    enabled: Boolean(dagId) && runIds.length > 0 && enabled,
    placeholderData: (prev) => prev,
    queryFn: () =>
      GridService.getGridTiSummariesBatch({
        dagId,
        requestBody: runIds,
      }),
    queryKey: ["grid", "ti-summaries-batch", dagId, runIds],
    refetchInterval: hasPendingState ? refetchInterval : false,
  });

  // Transform batch response into a map for easy lookup by run_id
  const summariesByRunId: Record<string, GridTISummaries> = useMemo(() => {
    if (!batchSummaries?.summaries) {
      return {};
    }

    return Object.fromEntries(batchSummaries.summaries.map((summary) => [summary.run_id, summary]));
  }, [batchSummaries]);

  return { data: summariesByRunId, ...rest };
};
