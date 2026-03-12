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

type TabItem = {
  icon: React.ReactNode;
  label: string;
  value: string;
};

const HITL_REVIEW_PLUGIN_TAB = "plugin/hitl-review";

export type UseHITLReviewTabsOptions = {
  enabled?: boolean;
  mapIndex?: number;
  refetchInterval?: number | false;
};

export const filterHITLReviewTabs = (tabs: Array<TabItem>, hasHitlData: boolean): Array<TabItem> =>
  tabs.filter((tab) => tab.value !== HITL_REVIEW_PLUGIN_TAB || hasHitlData);

const getBasePath = () => {
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);

  return new URL(baseUrl).pathname.replace(/\/$/u, "");
};

const buildHITLReviewSessionUrl = ({
  dagId,
  dagRunId,
  mapIndex,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => {
  const searchParams = new URLSearchParams({
    dag_id: dagId,
    map_index: mapIndex.toString(),
    run_id: dagRunId,
    task_id: taskId,
  });

  return `${getBasePath()}/hitl-review/sessions/find?${searchParams.toString()}`;
};

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

  const { data: hasHITLReviewSession = false, isLoading: isLoadingHITLReviewSession } = useQuery({
    enabled,
    queryFn: async () => {
      const response = await fetch(buildHITLReviewSessionUrl({ dagId, dagRunId, mapIndex, taskId }));

      if (response.ok) {
        return true;
      }

      if (response.status === 404) {
        return false;
      }

      throw new Error("Failed to fetch HITL review session");
    },
    queryKey: ["hitl-review-session", dagId, dagRunId, taskId, mapIndex],
    refetchInterval,
  });

  return {
    hasHITLReviewSession,
    isLoadingHITLReviewSession,
    tabs: filterHITLReviewTabs(tabs, hasHITLReviewSession),
  };
};
