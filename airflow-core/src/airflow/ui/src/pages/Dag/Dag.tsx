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
import { FiBarChart, FiCode } from "react-icons/fi";
import { LuChartColumn } from "react-icons/lu";
import { MdDetails, MdOutlineEventNote } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useDagServiceGetDagsUi } from "openapi/queries";
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { TaskIcon } from "src/assets/TaskIcon";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useRefreshOnNewDagRuns } from "src/queries/useRefreshOnNewDagRuns";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

const getTabs = (translate: (key: string) => string) => [
  { icon: <LuChartColumn />, label: translate("tabs.overview"), value: "" },
  { icon: <FiBarChart />, label: translate("tabs.runs"), value: "runs" },
  { icon: <TaskIcon />, label: translate("tabs.tasks"), value: "tasks" },
  { icon: <RiArrowGoBackFill />, label: translate("tabs.backfills"), value: "backfills" },
  { icon: <MdOutlineEventNote />, label: translate("tabs.events"), value: "events" },
  { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
  { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
];

export const Dag = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "" } = useParams();

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const refetchInterval = useAutoRefresh({ dagId });
  const [hasPendingRuns, setHasPendingRuns] = useState<boolean | undefined>(false);

  // TODO: replace with with a list dag runs by dag id request
  const {
    data: runsData,
    error: runsError,
    isLoading: isLoadingRuns,
  } = useDagServiceGetDagsUi({ dagIds: [dagId], dagRunsLimit: 1 }, undefined, {
    enabled: Boolean(dagId),
    refetchInterval: (query) => {
      setHasPendingRuns(
        query.state.data?.dags
          .find((recentDag) => recentDag.dag_id === dagId)
          ?.latest_dag_runs.some((run) => isStatePending(run.state)),
      );

      return hasPendingRuns ? refetchInterval : false;
    },
  });

  // Ensures continuous refresh to detect new runs when there's no pending state and new runs are initiated from other page
  useRefreshOnNewDagRuns(dagId, hasPendingRuns);

  let dagWithRuns = runsData?.dags.find((recentDag) => recentDag.dag_id === dagId);

  if (dagWithRuns === undefined && dag !== undefined) {
    dagWithRuns = {
      latest_dag_runs: [],
      ...dag,
    } satisfies DAGWithLatestDagRunsResponse;
  }

  return (
    <ReactFlowProvider>
      <DetailsLayout
        dag={dag}
        error={error ?? runsError}
        isLoading={isLoading || isLoadingRuns}
        tabs={getTabs(translate).filter(
          (tab) => !(dag?.timetable_summary === null && tab.value === "backfills"),
        )}
      >
        <Header
          dag={dag}
          dagWithRuns={dagWithRuns}
          isRefreshing={Boolean(
            dagWithRuns?.latest_dag_runs.some((dr) => isStatePending(dr.state)) && Boolean(refetchInterval),
          )}
        />
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
