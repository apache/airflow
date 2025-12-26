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
  useDagRunServiceDeleteDagRun,
  useDagRunServiceGetDagRunsKey,
  UseDagRunServiceGetDagRunKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServiceGetHitlDetailsKey,
} from "openapi/queries";
import { toaster } from "src/components/ui";

type DeleteDagRunParams = {
  dagId: string;
  dagRunId: string;
  onSuccessConfirm: () => void;
};

export const useDeleteDagRun = ({ dagId, dagRunId, onSuccessConfirm }: DeleteDagRunParams) => {
  const { t: translate } = useTranslation();
  const queryClient = useQueryClient();

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("dags:runAndTaskActions.delete.error", { type: translate("dagRun_one") }),
      type: "error",
    });
  };

  const onSuccess = async () => {
    const queryKeys = [
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetHitlDetailsKey],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    toaster.create({
      description: translate("dags:runAndTaskActions.delete.success.description", {
        type: translate("dagRun_one"),
      }),
      title: translate("dags:runAndTaskActions.delete.success.title", { type: translate("dagRun_one") }),
      type: "success",
    });

    onSuccessConfirm();
  };

  return useDagRunServiceDeleteDagRun({
    onError,
    onSuccess,
  });
};
