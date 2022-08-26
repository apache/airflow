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
import { useMutation } from 'react-query';
import type { TaskState } from 'src/types';
import URLSearchParamsWrapper from 'src/utils/URLSearchParamWrapper';
import { getMetaValue } from '../utils';
import useErrorToast from '../utils/useErrorToast';

const confirmUrl = getMetaValue('confirm_url');

export default function useConfirmMarkTask({
  dagId, runId, taskId, state,
}: { dagId: string, runId: string, taskId: string, state: TaskState }) {
  const errorToast = useErrorToast();
  return useMutation(
    ['confirmStateChange', dagId, runId, taskId, state],
    ({
      past, future, upstream, downstream, mapIndexes = [],
    }: {
      past: boolean,
      future: boolean,
      upstream: boolean,
      downstream: boolean,
      mapIndexes: number[],
    }) => {
      const params = new URLSearchParamsWrapper({
        dag_id: dagId,
        dag_run_id: runId,
        task_id: taskId,
        past,
        future,
        upstream,
        downstream,
        state,
      });

      mapIndexes.forEach((mi: number) => {
        params.append('map_index', mi.toString());
      });
      return axios.get<AxiosResponse, string[]>(confirmUrl, { params });
    },
    {
      onError: (error: Error) => errorToast({ error }),
    },
  );
}
