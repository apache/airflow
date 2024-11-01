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

import axios from "axios";
import { useQuery, UseQueryOptions } from "react-query";
import { useAutoRefresh } from "src/context/autorefresh";
import type {
  GetTaskInstanceTriesVariables,
  TaskInstanceCollection,
} from "src/types/api-generated";

import { getMetaValue } from "src/utils";

interface Props extends GetTaskInstanceTriesVariables {
  mapIndex?: number;
  options?: UseQueryOptions<TaskInstanceCollection>;
}

export default function useTIHistory({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
  options,
}: Props) {
  const { isRefreshOn } = useAutoRefresh();
  return useQuery<TaskInstanceCollection>(
    ["tiHistory", dagId, dagRunId, taskId, mapIndex],
    () => {
      let tiHistoryUrl = getMetaValue("task_tries_api")
        .replace("_DAG_ID_", dagId)
        .replace("_DAG_RUN_ID_", dagRunId)
        .replace("_TASK_ID_", taskId);

      if (mapIndex && mapIndex > -1) {
        tiHistoryUrl = tiHistoryUrl.replace("/tries", `/${mapIndex}/tries`);
      }

      return axios.get(tiHistoryUrl);
    },
    {
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
      ...options,
    }
  );
}
