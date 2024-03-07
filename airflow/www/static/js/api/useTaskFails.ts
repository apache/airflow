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

import { useQuery } from "react-query";
import axios, { AxiosResponse } from "axios";

import { getMetaValue } from "src/utils";
import { useAutoRefresh } from "src/context/autorefresh";

const DAG_ID_PARAM = "dag_id";
const RUN_ID_PARAM = "run_id";
const TASK_ID_PARAM = "task_id";

const dagId = getMetaValue(DAG_ID_PARAM);
const taskFailsUrl = getMetaValue("task_fails_url");

export interface TaskFail {
  runId: string;
  taskId: string;
  mapIndex?: number;
  startDate?: string;
  endDate?: string;
}

interface Props {
  runId?: string;
  taskId?: string;
  enabled?: boolean;
}

const useTaskFails = ({ runId, taskId, enabled = true }: Props) => {
  const { isRefreshOn } = useAutoRefresh();

  return useQuery(
    ["taskFails", runId, taskId],
    async () => {
      const params = {
        [DAG_ID_PARAM]: dagId,
        [RUN_ID_PARAM]: runId,
        [TASK_ID_PARAM]: taskId,
      };
      return axios.get<AxiosResponse, TaskFail[]>(taskFailsUrl, { params });
    },
    {
      enabled,
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
    }
  );
};

export default useTaskFails;
