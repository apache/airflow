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
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useDagRunServiceGetUpstreamAssetEvents } from "openapi/queries";
import { AssetEvents as AssetEventsTable } from "src/components/Assets/AssetEvents";
import { isStatePending, useAutoRefresh } from "src/utils";

export const AssetEvents = () => {
  const { dagId = "", runId = "" } = useParams();

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false) },
  );

  const { data, isLoading } = useDagRunServiceGetUpstreamAssetEvents({ dagId, dagRunId: runId }, undefined, {
    enabled: dagRun?.run_type === "asset_triggered",
    refetchInterval: () => (isStatePending(dagRun?.state) ? refetchInterval : false),
  });

  return <AssetEventsTable data={data} isLoading={isLoading} title="Source Asset Event" />;
};
