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

import axios, { AxiosResponse } from 'axios';
import { useQuery } from 'react-query';
import { useAutoRefresh } from 'src/context/autorefresh';
import type { API, TaskInstance } from 'src/types';

import { getMetaValue } from 'src/utils';

const taskLogApi = getMetaValue('task_log_api');

interface Props extends API.GetLogVariables {
  state?: TaskInstance['state'];
}

const useTaskLog = ({
  dagId, dagRunId, taskId, taskTryNumber, mapIndex, fullContent, state,
}: Props) => {
  let url: string = '';
  if (taskLogApi) {
    url = taskLogApi.replace('_DAG_RUN_ID_', dagRunId).replace('_TASK_ID_', taskId).replace(/-1$/, taskTryNumber.toString());
  }

  const { isRefreshOn } = useAutoRefresh();

  // Only refresh is the state is pending
  const isStatePending = state === 'deferred'
    || state === 'scheduled'
    || state === 'running'
    || state === 'up_for_reschedule'
    || state === 'up_for_retry'
    || state === 'queued'
    || state === 'restarting';

  return useQuery(
    ['taskLogs', dagId, dagRunId, taskId, mapIndex, taskTryNumber, fullContent, state],
    () => axios.get<AxiosResponse, string>(url, { headers: { Accept: 'text/plain' }, params: { map_index: mapIndex, full_content: fullContent } }),
    {
      placeholderData: '',
      refetchInterval: isStatePending && isRefreshOn && (autoRefreshInterval || 1) * 1000,
    },
  );
};

export default useTaskLog;
