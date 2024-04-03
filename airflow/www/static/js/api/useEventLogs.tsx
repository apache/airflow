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
import type { API } from "src/types";
import { useAutoRefresh } from "src/context/autorefresh";

export default function useEventLogs({
  dagId,
  taskId,
  runId,
  limit,
  offset,
  orderBy,
  after,
  before,
  owner,
  includedEvents,
  excludedEvents,
}: API.GetEventLogsVariables) {
  const { isRefreshOn } = useAutoRefresh();
  return useQuery(
    [
      "eventLogs",
      dagId,
      taskId,
      runId,
      limit,
      offset,
      orderBy,
      after,
      before,
      owner,
      excludedEvents,
      includedEvents,
    ],
    () => {
      const eventsLogUrl = getMetaValue("event_logs_api");
      const orderParam = orderBy ? { order_by: orderBy } : {};
      const excludedParam = excludedEvents
        ? { excluded_events: excludedEvents }
        : {};
      const includedParam = includedEvents
        ? { included_events: includedEvents }
        : {};
      return axios.get<AxiosResponse, API.EventLogCollection>(eventsLogUrl, {
        params: {
          offset,
          limit,
          ...{ dag_id: dagId },
          ...{ task_id: taskId },
          ...{ run_id: runId },
          ...orderParam,
          ...excludedParam,
          ...includedParam,
          after,
          before,
        },
      });
    },
    {
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
      keepPreviousData: true,
    }
  );
}
