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
import { useTranslation } from "react-i18next";
import { FiCode, FiDatabase } from "react-icons/fi";
import { MdDetails, MdOutlineEventNote, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useDagServiceGetDagDetails } from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const Run = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", runId = "" } = useParams();

  const tabs = [
    { icon: <MdOutlineTask />, label: translate("tabs.taskInstances"), value: "" },
    { icon: <FiDatabase />, label: translate("tabs.assetEvents"), value: "asset_events" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
    { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
  ];

  const refetchInterval = useAutoRefresh({ dagId });

  const {
    data: dag,
    error: dagError,
    isLoading: isLoadinDag,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const {
    data: dagRun,
    error,
    isLoading,
  } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    {
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  return (
    <ReactFlowProvider>
      <DetailsLayout dag={dag} error={error ?? dagError} isLoading={isLoading || isLoadinDag} tabs={tabs}>
        {dagRun === undefined ? undefined : (
          <Header
            dagRun={dagRun}
            isRefreshing={Boolean(isStatePending(dagRun.state) && Boolean(refetchInterval))}
          />
        )}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
