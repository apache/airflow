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
  useDagServiceBulkDags,
  useDagServiceGetDagDetailsKey,
  useDagServiceGetDagKey,
  useDagServiceGetDagsKey,
  useDagServiceGetDagsUiKey,
} from "openapi/queries";
import type { BulkBody_BulkDAGBody_, BulkResponse } from "openapi/requests/types.gen";

import { toaster } from "src/system-components";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly onSuccessConfirm: VoidFunction;
};

export const useBulkPauseDrainDags = ({ deselectKeys, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagsKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagsUiKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagServiceGetDagDetailsKey] }),
    ]);

    const updateResult = responseData.update;

    if (!updateResult) {
      return;
    }

    const successKeys = updateResult.success ?? [];
    const actionErrors = updateResult.errors ?? [];

    if (successKeys.length > 0) {
      toaster.create({
        description: translate("toaster.bulkUpdate.success.description", {
          count: successKeys.length,
          keys: successKeys.join(", "),
          resourceName: translate("dag_other"),
        }),
        title: translate("toaster.bulkUpdate.success.title", {
          resourceName: translate("dag_other"),
        }),
        type: "success",
      });
      deselectKeys(successKeys);
    }

    // Per-entity failures (status 200 with items in ``errors``) keep the dialog open
    // so the user can see what failed; the consumer renders ``data.update.errors``.
    if (actionErrors.length === 0) {
      onSuccessConfirm();
    }
  };

  const { data, error, isPending, mutate, reset } = useDagServiceBulkDags({ onSuccess });

  const bulkAction = (requestBody: BulkBody_BulkDAGBody_) => {
    reset();
    mutate({ requestBody });
  };

  return { bulkAction, data, error, isPending, reset };
};
