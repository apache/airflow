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
import { MdDetails, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useBackfillServiceGetBackfill } from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const Backfill = () => {
  const { t: translate } = useTranslation("dag");
  const { backfillId = "", dagId = "" } = useParams();
  const parsedBackfillId = Number(backfillId);
  const hasValidBackfillId = Number.isInteger(parsedBackfillId) && parsedBackfillId > 0;
  const refetchInterval = useAutoRefresh({ dagId });

  const tabs = [
    { icon: <MdOutlineTask />, label: translate("tabs.dagRuns"), value: "" },
    { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
  ];

  const {
    data: backfill,
    error,
    isLoading,
  } = useBackfillServiceGetBackfill({ backfillId: hasValidBackfillId ? parsedBackfillId : 0 }, undefined, {
    enabled: hasValidBackfillId,
    refetchInterval: (query) =>
      query.state.data?.completed_at === null && !query.state.data.is_paused ? refetchInterval : false,
  });

  return (
    <ReactFlowProvider>
      <DetailsLayout
        error={error}
        isLoading={isLoading}
        outletContext={backfill}
        showBackfillBanner={false}
        tabs={tabs}
      >
        {backfill === undefined ? undefined : <Header backfill={backfill} />}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
