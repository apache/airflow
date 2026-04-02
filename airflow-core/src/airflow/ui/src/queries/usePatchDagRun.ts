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
import { useTranslation } from "react-i18next";

import {
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  useDagRunServicePatchDagRun,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import { createErrorToaster } from "src/utils";

import { gridQueryKeys } from "./gridViewQueryKeys";
import { useClearDagRunDryRunKey } from "./useClearDagRunDryRun";

export const usePatchDagRun = ({
  dagId,
  dagRunId,
  onSuccess,
}: {
  dagId: string;
  dagRunId: string;
  onSuccess?: () => void;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      {
        params: { resourceName: translate("dagRun_one") },
        titleKey: "toaster.update.error",
      },
      translate,
    );
  };

  const onSuccessFn = async () => {
    const queryKeys = [
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey, { dagId, dagRunId }],
      [useClearDagRunDryRunKey, dagId],
    ];

    await Promise.all([
      ...gridQueryKeys(dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ...queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })),
    ]);

    if (onSuccess) {
      onSuccess();
    }
  };

  return useDagRunServicePatchDagRun({
    onError,
    onSuccess: onSuccessFn,
  });
};
