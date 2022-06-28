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
import { getMetaValue } from '../../utils';

const taskLogApi = getMetaValue('task_log_api');

const useTaskLog = ({
  dagId, dagRunId, taskId, taskTryNumber, fullContent,
}: {
  dagId: string,
  dagRunId: string,
  taskId: string,
  taskTryNumber: number,
  fullContent: boolean,
}) => {
  let url: string = '';
  if (taskLogApi) {
    url = taskLogApi.replace('_DAG_RUN_ID_', dagRunId).replace('_TASK_ID_', taskId).replace(/-1$/, taskTryNumber.toString());
  }

  return useQuery(
    ['taskLogs', dagId, dagRunId, taskId, taskTryNumber, fullContent],
    () => axios.get<AxiosResponse, string>(url, { headers: { Accept: 'text/plain' }, params: { full_content: fullContent } }),
    {
      placeholderData: '',
    },
  );
};

export default useTaskLog;
