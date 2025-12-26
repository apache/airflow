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
import { useState } from "react";
import { useTranslation } from "react-i18next";

import {
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  useTaskInstanceServiceGetHitlDetailsKey,
  useTaskInstanceServiceGetHitlDetailKey,
  useTaskInstanceServiceUpdateHitlDetail,
  useTaskInstanceServiceGetTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import { toaster } from "src/components/ui/Toaster";
import type { HITLResponseParams } from "src/utils/hitl";

export const useUpdateHITLDetail = ({
  dagId,
  dagRunId,
  mapIndex,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  mapIndex: number | undefined;
  taskId: string;
}) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation("hitl");
  const onSuccess = async () => {
    const queryKeys = [
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey, { dagId, dagRunId }],
      [useTaskInstanceServiceGetTaskInstanceKey, { dagId, dagRunId, mapIndex, taskId }],
      [useTaskInstanceServiceGetHitlDetailsKey, { dagIdPattern: dagId, dagRunId }],
      [useTaskInstanceServiceGetHitlDetailKey, { dagId, dagRunId }],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    toaster.create({
      title: translate("response.success", { taskId }),
      type: "success",
    });
  };

  const onError = (_error: Error) => {
    toaster.create({
      description: _error.message,
      title: translate("response.error"),
      type: "error",
    });
  };

  const { isPending, mutate } = useTaskInstanceServiceUpdateHitlDetail({
    onError,
    onSuccess,
  });

  const updateHITLResponse = (updateHITLResponseRequestBody: HITLResponseParams) => {
    try {
      mutate({
        dagId,
        dagRunId,
        mapIndex: mapIndex ?? -1,
        requestBody: {
          chosen_options: updateHITLResponseRequestBody.chosen_options ?? [],
          params_input: updateHITLResponseRequestBody.params_input ?? {},
        },
        taskId,
      });
    } catch (parseError) {
      setError(parseError);
    }
  };

  return { error, isPending, setError, updateHITLResponse };
};
