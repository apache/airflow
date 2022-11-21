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
import type { API, TaskInstance } from 'src/types';
import { useQuery } from 'react-query';
import { useAutoRefresh } from 'src/context/autorefresh';

import { getMetaValue } from 'src/utils';
import type { SetOptional } from 'type-fest';

/* GridData.TaskInstance and API.TaskInstance are not compatible at the moment.
 * Remove this function when changing the api response for grid_data_url to comply
 * with API.TaskInstance.
 */
const convertTaskInstance = (
  ti:
  API.TaskInstance,
) => ({ ...ti, runId: ti.dagRunId }) as TaskInstance;

const taskInstanceApi = getMetaValue('task_instance_api');

interface Props extends SetOptional<API.GetMappedTaskInstanceVariables, 'mapIndex'> {
  enabled: boolean;
}

const useTaskInstance = ({
  dagId, dagRunId, taskId, mapIndex, enabled,
}: Props) => {
  let url: string = '';
  if (taskInstanceApi) {
    url = taskInstanceApi.replace('_DAG_RUN_ID_', dagRunId).replace('_TASK_ID_', taskId || '');
  }

  if (mapIndex !== undefined && mapIndex >= 0) {
    url += `/${mapIndex.toString()}`;
  }

  const { isRefreshOn } = useAutoRefresh();

  return useQuery(
    ['taskInstance', dagId, dagRunId, taskId, mapIndex],
    () => axios.get<AxiosResponse, API.TaskInstance>(url, { headers: { Accept: 'text/plain' } }),
    {
      placeholderData: {},
      refetchInterval: isRefreshOn && (autoRefreshInterval || 1) * 1000,
      enabled,
      select: convertTaskInstance,
    },
  );
};

export default useTaskInstance;
