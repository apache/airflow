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

import axios, { AxiosResponse } from "axios";
import { useQuery } from "react-query";

import { getMetaValue } from "src/utils";
import { useAutoRefresh } from "src/context/autorefresh";
import type { API } from "src/types";

const mappedInstancesUrl = getMetaValue("mapped_instances_api");

export default function useMappedInstances({
  dagId,
  dagRunId,
  taskId,
  limit,
  offset,
  orderBy,
}: API.GetMappedTaskInstancesVariables) {
  const url = mappedInstancesUrl
    .replace("_DAG_RUN_ID_", dagRunId)
    .replace("_TASK_ID_", taskId);
  const orderParam =
    orderBy && orderBy !== "map_index" ? { order_by: orderBy } : {};
  const { isRefreshOn } = useAutoRefresh();
  return useQuery(
    ["mappedInstances", dagId, dagRunId, taskId, offset, orderBy],
    () =>
      axios.get<AxiosResponse, API.TaskInstanceCollection>(url, {
        params: { offset, limit, ...orderParam },
      }),
    {
      keepPreviousData: true,
      initialData: { taskInstances: [], totalEntries: 0 },
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
      // staleTime should be similar to the refresh interval
      staleTime: (autoRefreshInterval || 1) * 1000,
    }
  );
}
