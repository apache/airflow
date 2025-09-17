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
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate } from "react-router-dom";

import { useTaskInstanceServiceGetHitlDetails } from "openapi/queries";

export type HITLQueryParams = {
  dagId: string;
  dagRunId?: string;
  taskId?: string;
  taskIdPattern?: string;
};

export type TabItem = {
  icon: React.ReactNode;
  label: string;
  value: string;
};

export type UseRequiredActionTabsOptions = {
  autoRedirect?: boolean;
  refetchInterval?: number | false;
};

export const useRequiredActionTabs = (
  hitlParams: HITLQueryParams,
  tabs: Array<TabItem>,
  options: UseRequiredActionTabsOptions = {},
) => {
  const { t: translate } = useTranslation("hitl");
  const { autoRedirect = false, refetchInterval } = options;
  const location = useLocation();
  const navigate = useNavigate();

  const redirectPath = (() => {
    const { dagId, dagRunId, taskId, taskIdPattern } = hitlParams;

    if (Boolean(dagId) && Boolean(dagRunId) && Boolean(taskId)) {
      return `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}`;
    }
    if (Boolean(dagId) && Boolean(dagRunId)) {
      return `/dags/${dagId}/runs/${dagRunId}`;
    }
    if (Boolean(dagId) && Boolean(taskIdPattern)) {
      return `/dags/${dagId}/tasks/group/${taskIdPattern}`;
    }
    if (Boolean(dagId)) {
      return `/dags/${dagId}`;
    }

    // Fallback: remove /required_actions from current path
    return location.pathname.replace("/required_actions", "");
  })();

  const { data: hitlData, isLoading: isLoadingHitl } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId: hitlParams.dagId,
      dagRunId: hitlParams.dagRunId ?? "~",
      taskId: hitlParams.taskId,
      taskIdPattern: hitlParams.taskIdPattern,
    },
    undefined,
    {
      enabled: Boolean(hitlParams.dagId),
      refetchInterval,
    },
  );

  const hasHitlData = (hitlData?.total_entries ?? 0) > 0;
  const pendingActionsCount =
    hitlData?.hitl_details.filter(
      (hitl) => hitl.task_instance.state === "deferred" && !hitl.response_received,
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
      navigate(redirectPath);
    }
  }, [autoRedirect, hasHitlData, isLoadingHitl, location.pathname, navigate, redirectPath]);

  return {
    hasHitlData,
    hitlData,
    isLoadingHitl,
    pendingActionsCount,
    tabs: processedTabs,
  };
};
