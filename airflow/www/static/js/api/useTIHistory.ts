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
import { useAutoRefresh } from "src/context/autorefresh";
import type { TaskInstance } from "src/types/api-generated";

import { getMetaValue } from "src/utils";

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex?: number;
  enabled?: boolean;
}

export default function useTIHistory({
  dagId,
  runId,
  taskId,
  mapIndex = -1,
  enabled,
}: Props) {
  const { isRefreshOn } = useAutoRefresh();
  return useQuery(
    ["tiHistory", dagId, runId, taskId, mapIndex],
    () => {
      const tiHistoryUrl = getMetaValue("ti_history_url");

      const params = {
        dag_id: dagId,
        run_id: runId,
        task_id: taskId,
        map_index: mapIndex,
      };

      return axios.get<AxiosResponse, Partial<TaskInstance>[]>(tiHistoryUrl, {
        params,
      });
    },
    {
      enabled,
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
    }
  );
}
