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
  useDagServiceGetDagDetailsKey,
  useDagServiceGetDagsUiKey,
  useDagServiceGetLatestRunInfo,
} from "openapi/queries";

import { gridQueryKeys } from "./gridViewQueryKeys";
import { useConfig } from "./useConfig";

export const useRefreshOnNewDagRuns = (dagId: string, hasPendingRuns: boolean | undefined) => {
  const queryClient = useQueryClient();
  const hasSyncedLatestRunRef = useRef(false);
  const previousLatestRunSignatureRef = useRef<string>("");
  const autoRefreshInterval = useConfig("auto_refresh_interval") as number;

  const pollIntervalMs = Boolean(autoRefreshInterval) ? autoRefreshInterval * 1000 : 5000;

  const { data: latestDagRun } = useDagServiceGetLatestRunInfo({ dagId }, undefined, {
    enabled: Boolean(dagId),
    refetchInterval: Boolean(dagId) && !hasPendingRuns ? pollIntervalMs : false,
  });

  useEffect(() => {
    hasSyncedLatestRunRef.current = false;
    previousLatestRunSignatureRef.current = "";
  }, [dagId]);

  useEffect(() => {
    if (!dagId) {
      return;
    }

    if (latestDagRun === undefined) {
      return;
    }

    const signature = latestDagRun?.run_id ?? "";

    if (!hasSyncedLatestRunRef.current) {
      hasSyncedLatestRunRef.current = true;
      previousLatestRunSignatureRef.current = signature;

      return;
    }

    if (previousLatestRunSignatureRef.current === signature) {
      return;
    }

    previousLatestRunSignatureRef.current = signature;

    void Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagsUiKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagDetailsKey] }),
      ...gridQueryKeys(dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
    ]);
  }, [dagId, latestDagRun, queryClient]);
};
