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

import axios from 'axios';
import { useMutation, useQueryClient } from 'react-query';
import { getMetaValue } from '../../utils';

export default function useClearRun(dagId, runId) {
  const queryClient = useQueryClient();
  return useMutation(
    ['dagRunClear', dagId, runId],
    () => {
      const csrfToken = getMetaValue('csrf_token');
      const params = new URLSearchParams({
        csrf_token: csrfToken,
        confirmed: true,
        dag_id: dagId,
        dag_run_id: runId,
      }).toString();

      return axios.post('/dagrun_clear', params, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
      });
    },
    {
      onSettled: () => {
        queryClient.invalidateQueries('treeData');
      },
    },
  );
}
