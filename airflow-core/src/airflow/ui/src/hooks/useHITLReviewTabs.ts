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
import axios, { type AxiosError } from "axios";
import { useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { OpenAPI } from "openapi/requests/core/OpenAPI";
import type { TabItem } from "src/hooks/useRequiredActionTabs";

const HITL_REVIEW_PLUGIN_TAB = "plugin/hitl-review";

export type UseHITLReviewTabsOptions = {
  enabled?: boolean;
  mapIndex?: number;
  refetchInterval?: number | false;
};

const filterHITLReviewTabs = (tabs: Array<TabItem>, hasHitlData: boolean): Array<TabItem> =>
  tabs.filter((tab) => tab.value !== HITL_REVIEW_PLUGIN_TAB || hasHitlData);

export const useHITLReviewTabs = (
  {
    dagId,
    dagRunId,
    taskId,
  }: {
    dagId: string;
    dagRunId: string;
    taskId: string;
  },
  tabs: Array<TabItem>,
  options: UseHITLReviewTabsOptions = {},
) => {
  const { enabled = true, mapIndex = -1, refetchInterval } = options;
  const hasHitlTab = tabs.some((tab) => tab.value === HITL_REVIEW_PLUGIN_TAB);
  const location = useLocation();
  const navigate = useNavigate();
  const redirectPath =
    Boolean(dagId) && Boolean(dagRunId) && Boolean(taskId)
      ? `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}`
      : location.pathname.replace("/plugin/hitl-review", "");

  const { data: hasHITLReviewSession = false, isLoading: isLoadingHITLReviewSession } = useQuery({
    enabled: enabled && hasHitlTab,
    queryFn: () =>
      axios
        .get(`${OpenAPI.BASE}/hitl-review/sessions/find`, {
          params: {
            dag_id: dagId,
            map_index: mapIndex,
            run_id: dagRunId,
            task_id: taskId,
          },
        })
        .then(() => true)
        .catch((error: unknown) => {
          if (!axios.isAxiosError(error)) {
            return Promise.reject(error);
          }

          const status = error.response?.status;

          if (status === 404) {
            return false;
          }

          // queryClient.ts reads error.status to decide whether 4xx responses should be retried.
          if (status !== undefined) {
            (error as { status?: number } & AxiosError).status = status;
          }

          return Promise.reject(error);
        }),
    queryKey: ["hitl-review-session", dagId, dagRunId, taskId, mapIndex],
    refetchInterval,
  });

  useEffect(() => {
    if (
      !hasHITLReviewSession &&
      !isLoadingHITLReviewSession &&
      location.pathname.includes("plugin/hitl-review")
    ) {
      void Promise.resolve(navigate(redirectPath));
    }
  }, [hasHITLReviewSession, isLoadingHITLReviewSession, location.pathname, navigate, redirectPath]);

  return {
    hasHITLReviewSession,
    isLoadingHITLReviewSession,
    tabs: filterHITLReviewTabs(tabs, hasHITLReviewSession),
  };
};
