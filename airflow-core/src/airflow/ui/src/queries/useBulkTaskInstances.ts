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
  useTaskInstanceServiceGetTaskInstancesKey,
  UseGridServiceGetGridRunsKeyFn,
  UseGridServiceGetGridTiSummariesKeyFn,
  useGridServiceGetGridTiSummariesKey,
  useTaskInstanceServiceBulkTaskInstances,
} from "openapi/queries";
import { toaster } from "src/components/ui";

import { BulkBody_BulkTaskInstanceBody_, BulkUpdateAction_BulkTaskInstanceBody_ } from "openapi/requests/types.gen";

export const useBulkTaskInstances = ({
  dagId,
  runId,
  onSuccess,
}: {
  dagId: string;
  runId: string;
  onSuccess: () => void;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("toaster.update.error", {
        resourceName: translate("taskGroup"),
      }),
      type: "error",
    });
  };

  const onSuccessFn = async (
    _: unknown,
    variables: {
      dagId: string;
      dagRunId: string;
      requestBody: BulkBody_BulkTaskInstanceBody_;
    },
  ) => {
    const { requestBody } = variables;
    const { actions } = requestBody;
    const updateActions = actions.filter(
      (action) => action.action === "update",
    ) as BulkUpdateAction_BulkTaskInstanceBody_[];
    const updateActionsEntities = updateActions.flatMap((action) => action.entities);
    const affectsMultipleRuns = updateActionsEntities.some(
      (entity) => entity.include_future === true || entity.include_past === true,
    );

    const queryKeys = [
      [useTaskInstanceServiceGetTaskInstancesKey],
      UseGridServiceGetGridRunsKeyFn({ dagId }, [{ dagId }]),
      affectsMultipleRuns
        ? [useGridServiceGetGridTiSummariesKey, { dagId }]
        : UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId }),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    if (onSuccess) {
      onSuccess();
    }
  };

  return useTaskInstanceServiceBulkTaskInstances({
    onError,
    onSuccess: onSuccessFn,
  });
};
