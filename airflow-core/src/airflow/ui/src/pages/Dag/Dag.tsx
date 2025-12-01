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
import { ReactFlowProvider } from "@xyflow/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiBarChart, FiCode, FiUser, FiCalendar } from "react-icons/fi";
import { LuChartColumn } from "react-icons/lu";
import { MdDetails, MdOutlineEventNote } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useDagServiceGetLatestRunInfo } from "openapi/queries";
import { TaskIcon } from "src/assets/TaskIcon";
import { usePluginTabs } from "src/hooks/usePluginTabs";
import { useRequiredActionTabs } from "src/hooks/useRequiredActionTabs";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useRefreshOnNewDagRuns } from "src/queries/useRefreshOnNewDagRuns";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const Dag = () => {
  const { t: translate } = useTranslation(["dag", "hitl"]);
  const { dagId = "" } = useParams();

  // Get external views with dag destination
  const externalTabs = usePluginTabs("dag");

  const tabs = [
    { icon: <LuChartColumn />, label: translate("tabs.overview"), value: "" },
    { icon: <FiBarChart />, label: translate("tabs.runs"), value: "runs" },
    { icon: <TaskIcon />, label: translate("tabs.tasks"), value: "tasks" },
    { icon: <FiCalendar />, label: translate("tabs.calendar"), value: "calendar" },
    { icon: <FiUser />, label: translate("tabs.requiredActions"), value: "required_actions" },
    { icon: <RiArrowGoBackFill />, label: translate("tabs.backfills"), value: "backfills" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
    { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
    ...externalTabs,
  ];

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const refetchInterval = useAutoRefresh({ dagId });
  const [hasPendingRuns, setHasPendingRuns] = useState<boolean | undefined>(false);

  // Ensures continuous refresh to detect new runs when there's no
  // pending state and new runs are initiated from other page
  useRefreshOnNewDagRuns(dagId, hasPendingRuns);

  const {
    data: latestRun,
    error: runsError,
    isLoading: isLoadingRuns,
  } = useDagServiceGetLatestRunInfo(
    {
      dagId,
    },
    undefined,
    {
      enabled: Boolean(dagId),
      refetchInterval: (query) => {
        if (query.state.data && isStatePending(query.state.data.state)) {
          setHasPendingRuns(true);
        }

        return hasPendingRuns ? refetchInterval : false;
      },
    },
  );

  const { tabs: processedTabs } = useRequiredActionTabs({ dagId }, tabs, {
    refetchInterval,
  });

  const displayTabs = processedTabs.filter((tab) => {
    if (dag?.timetable_summary === null && tab.value === "backfills") {
      return false;
    }

    return true;
  });

  return (
    <ReactFlowProvider>
      <DetailsLayout error={error ?? runsError} isLoading={isLoading || isLoadingRuns} tabs={displayTabs}>
        <Header dag={dag} latestRunInfo={latestRun ?? undefined} />
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
