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
import { useEffect, type ReactNode } from "react";

import { useTranslation } from "react-i18next";
import { useLocation, useNavigate } from "react-router-dom";

import { useTaskInstanceServiceGetHitlDetails } from "openapi/queries";

export type HITLQueryParams = {
  dagId: string;
  dagRunId?: string;
  mapIndex?: number;
  regionId?: string;
  regionIndex?: number;
  taskId?: string;
  taskIdPattern?: string;
};

export type TabItem = {
  icon: ReactNode;
  label: string;
  value: string;
};

export type UseRequiredActionTabsOptions = {
  autoRedirect?: boolean;
  enabled?: boolean;
  refetchInterval?: number | false;
};

export const useRequiredActionTabs = (
  hitlParams: HITLQueryParams,
  tabs: Array<TabItem>,
  options: UseRequiredActionTabsOptions = {},
) => {
  const { t: translate } = useTranslation("hitl");
  const { autoRedirect = false, enabled = true, refetchInterval } = options;
  const location = useLocation();
  const navigate = useNavigate();

  const { dagId, dagRunId, mapIndex, regionId, regionIndex, taskId, taskIdPattern } = hitlParams;
  const exact = regionId !== undefined && regionIndex !== undefined && taskId !== undefined;
  let redirectPath: string;

  if (Boolean(dagId) && Boolean(dagRunId) && Boolean(taskId)) {
    redirectPath = `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}`;
  } else if (Boolean(dagId) && Boolean(dagRunId)) {
    redirectPath = `/dags/${dagId}/runs/${dagRunId}`;
  } else if (Boolean(dagId) && Boolean(taskIdPattern)) {
    redirectPath = `/dags/${dagId}/tasks/group/${taskIdPattern}`;
  } else if (Boolean(dagId)) {
    redirectPath = `/dags/${dagId}`;
  } else {
    // Fallback: remove /required_actions from current path
    redirectPath = location.pathname.replace("/required_actions", "");
  }

  // Both the broad and the exact lookup go through the collection endpoint. Asking the single-detail
  // endpoint whether one task instance is HITL answers "no" with a 404, which every non-HITL task
  // then logs as a failed request; the collection answers the same question with `total_entries: 0`.
  const collection = useTaskInstanceServiceGetHitlDetails(
    {
      dagId,
      dagRunId: dagRunId ?? "~",
      mapIndex: exact ? (mapIndex ?? -1) : undefined,
      regionId: exact ? regionId : undefined,
      regionIndex: exact ? regionIndex : undefined,
      taskId,
      taskIdPrefixPattern: taskIdPattern,
    },
    undefined,
    {
      enabled: enabled && Boolean(dagId),
      refetchInterval,
    },
  );
  const hitlData = collection.data;
  const isLoadingHitl = collection.isLoading;

  const hasHitlData = enabled && (hitlData?.total_entries ?? 0) > 0;
  const pendingActionsCount =
    hitlData?.hitl_details.filter(
      (hitl) =>
        (hitl.task_instance.state === "deferred" || hitl.task_instance.state === "awaiting_input") &&
        !hitl.response_received,
    ).length ?? 0;

  const processedTabs = tabs
    .filter((tab) => {
      // Hide required_actions tab if no HITL data exists
      if (tab.value === "required_actions" && !hasHitlData) {
        return false;
      }

      return true;
    })
    .map((tab) => {
      // Update required_actions label with pending count
      if (tab.value === "required_actions" && pendingActionsCount > 0) {
        return {
          ...tab,
          label: translate("requiredActionCount", { count: pendingActionsCount }),
        };
      }

      return tab;
    });

  useEffect(() => {
    if (autoRedirect && !hasHitlData && !isLoadingHitl && location.pathname.includes("required_actions")) {
      void Promise.resolve(
        navigate(
          exact
            ? { pathname: location.pathname.replace("/required_actions", ""), search: location.search }
            : redirectPath,
        ),
      );
    }
  }, [
    autoRedirect,
    exact,
    hasHitlData,
    isLoadingHitl,
    location.pathname,
    location.search,
    navigate,
    redirectPath,
  ]);

  return {
    hasHitlData,
    hitlData,
    isLoadingHitl,
    pendingActionsCount,
    tabs: processedTabs,
  };
};
