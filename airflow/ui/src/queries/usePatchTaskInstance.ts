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
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
  UseTaskInstanceServiceGetTaskInstanceKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServicePatchTaskInstance,
} from "openapi/queries";
import { toaster } from "src/components/ui";

const onError = () => {
  toaster.create({
    description: "Patch Task Instance request failed",
    title: "Failed to patch the Task Instance",
    type: "error",
  });
};

export const usePatchTaskInstance = ({
  dagId,
  dagRunId,
  mapIndex,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => {
  const queryClient = useQueryClient();

  const onSuccess = async () => {
    const queryKeys = [
      UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }),
      UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }),
      [useTaskInstanceServiceGetTaskInstancesKey],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));
  };

  return useTaskInstanceServicePatchTaskInstance({
    onError,
    onSuccess,
  });
};
