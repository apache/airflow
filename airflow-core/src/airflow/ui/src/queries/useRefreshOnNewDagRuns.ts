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
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef } from "react";

import {
  useDagRunServiceGetDagRuns,
  useDagServiceGetDagDetailsKey,
  UseDagRunServiceGetDagRunsKeyFn,
  UseDagServiceGetDagDetailsKeyFn,
  useDagServiceRecentDagRunsKey,
  UseGridServiceGridDataKeyFn,
  UseTaskInstanceServiceGetTaskInstancesKeyFn,
} from "openapi/queries";

import { useConfig } from "./useConfig";

export const useRefreshOnNewDagRuns = (dagId: string, hasPendingRuns: boolean | undefined) => {
  const queryClient = useQueryClient();
  const previousDagRunIdRef = useRef<string>();
  const autoRefreshInterval = useConfig("auto_refresh_interval") as number;

  const { data } = useDagRunServiceGetDagRuns({ dagId, limit: 1, orderBy: "-run_after" }, undefined, {
    enabled: Boolean(dagId) && !hasPendingRuns,
    refetchInterval: Boolean(autoRefreshInterval) ? autoRefreshInterval * 1000 : 5000,
  });

  useEffect(() => {
    const latestDagRun = data?.dag_runs[0];

    const latestDagRunId = latestDagRun?.dag_run_id;

    if ((latestDagRunId ?? "") && previousDagRunIdRef.current !== latestDagRunId) {
      previousDagRunIdRef.current = latestDagRunId;

      const queryKeys = [
        [useDagServiceRecentDagRunsKey],
        [useDagServiceGetDagDetailsKey],
        UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }]),
        UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
        UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
        UseGridServiceGridDataKeyFn({ dagId }, [{ dagId }]),
      ];

      queryKeys.forEach((key) => {
        void queryClient.invalidateQueries({ queryKey: key });
      });
    }
  }, [data, dagId, queryClient]);
};
