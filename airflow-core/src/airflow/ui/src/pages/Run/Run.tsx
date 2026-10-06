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
import { FiCode, FiDatabase, FiPhoneCall } from "react-icons/fi";
import { MdDetails, MdOutlineEventNote, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useDeadlinesServiceGetDeadlines } from "openapi/queries";

import { DetailsLayout } from "src/layouts/Details/DetailsLayout";

import { usePluginTabs } from "src/hooks/usePluginTabs";
import { isStatePending, useAutoRefresh, useDocumentTitle } from "src/utils";

import { Header } from "./Header";

export const Run = () => {
  const { t: translate } = useTranslation(["dag", "hitl"]);
  const { dagId = "", runId = "" } = useParams();

  useDocumentTitle(runId);

  // Get external views with dag_run destination
  const externalTabs = usePluginTabs("dag_run");

  const refetchInterval = useAutoRefresh({ dagId });

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
      refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
    },
  );

  // Like the Required Actions tab, the Callbacks tab is only shown when the run has callbacks
  // (every deadline has one).
  const { data: deadlines } = useDeadlinesServiceGetDeadlines(
    { dagId, dagRunId: runId, limit: 1 },
    undefined,
    {
      refetchInterval: isStatePending(dagRun?.state) && refetchInterval,
    },
  );
  const hasCallbacks = (deadlines?.total_entries ?? 0) > 0;

  const tabs = [
    { icon: <MdOutlineTask />, label: translate("tabs.taskInstances"), value: "" },
    { icon: <FiDatabase />, label: translate("tabs.assetEvents"), value: "asset_events" },
    ...(hasCallbacks
      ? [
          // Also active on a callback's logs (callbacks/:callbackId/logs).
          {
            icon: <FiPhoneCall />,
            label: translate("tabs.callbacks"),
            matchPaths: ["logs"],
            value: "callbacks",
          },
        ]
      : []),
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
    { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
    ...externalTabs,
  ];

  return (
    <ReactFlowProvider>
      <DetailsLayout error={error} isLoading={isLoading} tabs={tabs}>
        {dagRun === undefined ? undefined : <Header dagRun={dagRun} />}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
