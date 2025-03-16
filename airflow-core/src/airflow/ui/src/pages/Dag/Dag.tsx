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
import { FiBarChart, FiCode } from "react-icons/fi";
import { LuChartColumn } from "react-icons/lu";
import { MdDetails, MdOutlineEventNote } from "react-icons/md";
import { RiArrowGoBackFill } from "react-icons/ri";
import { useParams } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagsServiceRecentDagRuns,
  useDagWarningServiceListDagWarnings,
} from "openapi/queries";
import { TaskIcon } from "src/assets/TaskIcon";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

const tabs = [
  { icon: <LuChartColumn />, label: "Overview", value: "" },
  { icon: <FiBarChart />, label: "Runs", value: "runs" },
  { icon: <TaskIcon />, label: "Tasks", value: "tasks" },
  { icon: <RiArrowGoBackFill />, label: "Backfills", value: "backfills" },
  { icon: <MdOutlineEventNote />, label: "Events", value: "events" },
  { icon: <FiCode />, label: "Code", value: "code" },
  { icon: <MdDetails />, label: "Details", value: "details" },
];

export const Dag = () => {
  const { dagId = "" } = useParams();

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: warningData, isLoading: isWarningLoading } = useDagWarningServiceListDagWarnings({
    dagId,
  });

  const refetchInterval = useAutoRefresh({ dagId });

  // TODO: replace with with a list dag runs by dag id request
  const {
    data: runsData,
    error: runsError,
    isLoading: isLoadingRuns,
  } = useDagsServiceRecentDagRuns({ dagIds: [dagId] }, undefined, {
    enabled: Boolean(dagId),
    refetchInterval: (query) =>
      query.state.data?.dags
        .find((recentDag) => recentDag.dag_id === dagId)
        ?.latest_dag_runs.some((run) => isStatePending(run.state))
        ? refetchInterval
        : false,
  });

  const dagWithRuns = runsData?.dags.find((recentDag) => recentDag.dag_id === dagId);

  return (
    <ReactFlowProvider>
      <DetailsLayout
        dag={dag}
        error={error ?? runsError}
        isLoading={isLoading || isLoadingRuns || isWarningLoading}
        tabs={tabs.filter((tab) => !(dag?.timetable_summary === null && tab.value === "backfills"))}
        warning={warningData}
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
