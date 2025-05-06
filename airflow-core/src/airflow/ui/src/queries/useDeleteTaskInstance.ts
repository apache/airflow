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
import { useQueryClient } from "@tanstack/react-query";

import {
  useTaskInstanceServiceDeleteTaskInstance,
  useTaskInstanceServiceGetTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
  UseGridServiceGridDataKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseDagRunServiceGetDagRunKeyFn,
} from "openapi/queries";
import { toaster } from "src/components/ui";

type DeleteTaskInstanceParams = {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  onSuccessConfirm: () => void;
  taskId: string;
};

const onError = () => {
  toaster.create({
    description: "Delete Task Instance request failed.",
    title: "Failed to delete Task Instance",
    type: "error",
  });
};

export const useDeleteTaskInstance = ({
  dagId,
  dagRunId,
  mapIndex,
  onSuccessConfirm,
  taskId,
}: DeleteTaskInstanceParams) => {
  const queryClient = useQueryClient();

  const onSuccess = async () => {
    const queryKeys = [
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetTaskInstanceKey, { dagId, dagRunId, mapIndex, taskId }],
      UseGridServiceGridDataKeyFn({ dagId }, [{ dagId }]),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    toaster.create({
      description: "The Task Instance deletion request was successful.",
      title: "Task Instance Deleted Successfully",
      type: "success",
    });

    onSuccessConfirm();
  };

  return useTaskInstanceServiceDeleteTaskInstance({
    onError,
    onSuccess,
  });
};
