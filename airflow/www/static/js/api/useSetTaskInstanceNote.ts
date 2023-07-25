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
import { useMutation, useQueryClient } from "react-query";

import { getMetaValue } from "src/utils";
import useErrorToast from "src/utils/useErrorToast";

import type { API } from "src/types";

const setTaskInstancesNoteURI = getMetaValue("set_task_instance_note");
const setMappedTaskInstancesNoteURI = getMetaValue(
  "set_mapped_task_instance_note"
);

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  mapIndex?: number;
}

export default function useSetTaskInstanceNote({
  dagId,
  runId,
  taskId,
  mapIndex = -1,
}: Props) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();
  // Note: Werkzeug does not like the META URL on dag.html with an integer. It can not put
  // _MAP_INDEX_ there as it interprets that as the integer. Hence, we pass 0 as the integer.
  // To avoid we replace other stuff, we add the surrounding strings to the replacement query.
  const url = (
    mapIndex >= 0 ? setMappedTaskInstancesNoteURI : setTaskInstancesNoteURI
  )
    .replace("_DAG_RUN_ID_", runId)
    .replace("_TASK_ID_/0/setNote", `_TASK_ID_/${mapIndex}/setNote`)
    .replace("_TASK_ID_", taskId);

  return useMutation(
    ["setTaskInstanceNotes", dagId, runId, taskId, mapIndex],
    (note: string | null) =>
      axios.patch<AxiosResponse, API.TaskInstance>(url, { note }),
    {
      onSuccess: async (data) => {
        const note = data.note ?? null;

        const updateMappedInstancesResult = (
          oldMappedInstances?: API.TaskInstanceCollection
        ) => {
          if (!oldMappedInstances) {
            return {
              taskInstances: [],
              totalEntries: 0,
            };
          }
          if (mapIndex === undefined || mapIndex < 0) return oldMappedInstances;
          return {
            ...oldMappedInstances,
            taskInstances: oldMappedInstances.taskInstances?.map((ti) =>
              ti.dagRunId === runId &&
              ti.taskId === taskId &&
              ti.mapIndex === mapIndex
                ? { ...ti, note }
                : ti
            ),
          };
        };

        const updateTaskInstanceResult = (
          oldTaskInstance?: API.TaskInstance
        ) => {
          if (!oldTaskInstance) throw new Error("Unknown value...");
          if (
            oldTaskInstance.dagRunId === runId &&
            oldTaskInstance.taskId === taskId &&
            ((oldTaskInstance.mapIndex == null && mapIndex < 0) ||
              oldTaskInstance.mapIndex === mapIndex)
          ) {
            return {
              ...oldTaskInstance,
              note,
            };
          }
          return oldTaskInstance;
        };
        /*
          This will force a refetch of gridData.
          Mutating the nested object is quite complicated,
          we should simplify the gridData API object first
        */
        await queryClient.invalidateQueries("gridData");

        if (mapIndex >= 0) {
          await queryClient.cancelQueries("mappedInstances");
          queryClient.setQueriesData(
            "mappedInstances",
            updateMappedInstancesResult
          );
        }

        await queryClient.cancelQueries("taskInstance");
        queryClient.setQueriesData(
          ["taskInstance", dagId, runId, taskId, mapIndex],
          updateTaskInstanceResult
        );
      },
      onError: (error: Error) => errorToast({ error }),
    }
  );
}
