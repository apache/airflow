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
import { getMetaValue, getTask } from 'src/utils';
import type { GridData } from 'src/api/useGridData';
import { emptyGridData } from 'src/api/useGridData';
import type { API } from 'src/types';
import useErrorToast from '../utils/useErrorToast';

const setTaskInstancesNotesURI = getMetaValue('set_task_instance_notes');

// Note: Not using API.SetTaskInstanceNotesVariables because the parameters in the body
// are interpreted as optional due to `openapi-typescript` (which they are not..).
export default function useSetTaskInstanceNotes(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
  newNotesValue: string,
) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();
  // Note: Werkzeug does not like the META URL with an integer. It can not put _MAP_INDEX_ there
  // as it interprets that as the integer. Hence, we pass -1 as the integer. To avoid we replace
  // other stuff, we add _TASK_ID_ to the replacement query.
  const url = setTaskInstancesNotesURI
    .replace('_DAG_RUN_ID_', runId)
    .replace('_TASK_ID_', taskId);

  const updateGridDataResult = (oldValue: GridData | undefined) => {
    if (oldValue == null) return emptyGridData;
    if (mapIndex !== undefined && mapIndex >= 0) return oldValue;
    const group = getTask({ taskId, task: oldValue.groups });
    const instance = group?.instances.find((ti) => ti.runId === runId);
    if (instance) {
      instance.notes = newNotesValue;
    }
    return oldValue;
  };

  const updateMappedInstancesResult = (oldValue: API.TaskInstanceCollection | undefined) => {
    if (oldValue == null) {
      return {
        taskInstances: undefined,
        totalEntries: 0,
      };
    }
    if (mapIndex === undefined || mapIndex < 0) return oldValue;
    const instance = oldValue?.taskInstances?.find(
      (ti) => (
        ti.dagRunId === runId && ti.taskId === taskId && ti.mapIndex === mapIndex
      ),
    );
    if (instance) {
      instance.notes = newNotesValue;
    }
    return oldValue;
  };

  const updateTaskInstanceResult = (oldValue: API.TaskInstance | undefined) => {
    if (oldValue == null) throw new Error('Unknown value..');
    if (oldValue.dagRunId === runId && oldValue.taskId === taskId) {
      if ((oldValue.mapIndex == null && mapIndex < 0) || oldValue.mapIndex === mapIndex) {
        oldValue.notes = newNotesValue;
      }
    }
    return oldValue;
  };

  const params = { map_index: mapIndex, notes: newNotesValue };
  return useMutation(
    ['setTaskInstanceNotes', dagId, runId],
    () => axios.patch(url, params),
    {
      onSuccess: async () => {
        await queryClient.cancelQueries('gridData');
        queryClient.setQueriesData('gridData', updateGridDataResult);

        await queryClient.cancelQueries('mappedInstances');
        queryClient.setQueriesData('mappedInstances', updateMappedInstancesResult);

        await queryClient.cancelQueries('taskInstance');
        queryClient.setQueriesData(
          ['taskInstance', dagId, runId, taskId, mapIndex],
          updateTaskInstanceResult,
        );
      },
      onError: (error: Error) => errorToast({ error }),
    },
  );
}
