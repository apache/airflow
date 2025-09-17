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

import { useAssetServiceGetAssetEvents, useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { AssetEvents as AssetEventsTable } from "src/components/Assets/AssetEvents";
import { isStatePending, useAutoRefresh } from "src/utils";

export const AssetEvents = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
    },
  );

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: assetEventsData, isLoading } = useAssetServiceGetAssetEvents(
    {
      sourceDagId: dagId,
      sourceMapIndex: parseInt(mapIndex, 10),
      sourceRunId: runId,
      sourceTaskId: taskId,
    },
    undefined,
    {
      refetchInterval: () => (isStatePending(taskInstance?.state) ? refetchInterval : false),
    },
  );

  return (
    <AssetEventsTable data={assetEventsData} isLoading={isLoading} titleKey="common:createdAssetEvent" />
  );
};
