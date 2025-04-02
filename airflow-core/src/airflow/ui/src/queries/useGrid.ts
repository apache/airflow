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
import { keepPreviousData } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useGridServiceGridData } from "openapi/queries";
import type { GridResponse } from "openapi/requests/types.gen";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useGrid = (limit: number) => {
  const { dagId = "", runId = "" } = useParams();
  const [runAfter, setRunAfter] = useState<string | undefined>();

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { enabled: runId !== "" },
  );

  const refetchInterval = useAutoRefresh({ dagId });

  // This is necessary for keepPreviousData
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-arguments
  const { data: gridData, ...rest } = useGridServiceGridData<GridResponse>(
    {
      dagId,
      limit,
      orderBy: "-run_after",
      runAfterLte: runAfter,
    },
    undefined,
    {
      placeholderData: keepPreviousData,
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((dr) => isStatePending(dr.state)) && refetchInterval,
    },
  );

  // Check if the selected dag run is inside of the grid response, if not, we'll update the grid filters
  // Eventually we should redo the api endpoint to make this work better
  useEffect(() => {
    if (gridData?.dag_runs && dagRun) {
      const hasRun = gridData.dag_runs.find((dr) => dr.dag_run_id === dagRun.dag_run_id);

      if (!hasRun) {
        setRunAfter(dagRun.run_after);
      }
    }
  }, [dagRun, gridData?.dag_runs, runAfter]);

  return { data: gridData, runAfter, ...rest };
};
