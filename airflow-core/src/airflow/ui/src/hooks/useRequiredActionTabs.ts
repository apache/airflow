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

  const { dagId, dagRunId, taskId, taskIdPattern } = hitlParams;
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

  const { data: hitlData, isLoading: isLoadingHitl } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId,
      dagRunId: dagRunId ?? "~",
      taskId,
      taskIdPattern,
    },
    undefined,
    {
      enabled: Boolean(dagId),
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
      void Promise.resolve(navigate(redirectPath));
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
