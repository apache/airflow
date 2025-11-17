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

import { usePoolServiceGetPoolsKey, usePoolServicePatchPool } from "openapi/queries";
import type { PoolBody } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

export const useEditPool = (
  initialPool: PoolBody,
  {
    onSuccessConfirm,
  }: {
    onSuccessConfirm: () => void;
  },
) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation(["common", "admin"]);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [usePoolServiceGetPoolsKey],
    });

    toaster.create({
      description: translate("toaster.update.success.description", {
        resourceName: translate("admin:pools.pool_one"),
      }),
      title: translate("toaster.update.success.title", {
        resourceName: translate("admin:pools.pool_one"),
      }),
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = usePoolServicePatchPool({
    onError,
    onSuccess,
  });

  const editPool = (editPoolRequestBody: PoolBody) => {
    const updateMask: Array<string> = [];
    let parsedDescription = undefined;

    if (editPoolRequestBody.slots !== initialPool.slots) {
      updateMask.push("slots");
    }
    if (editPoolRequestBody.description !== initialPool.description) {
      parsedDescription = editPoolRequestBody.description;
      updateMask.push("description");
    }
    if (editPoolRequestBody.include_deferred !== initialPool.include_deferred) {
      updateMask.push("include_deferred");
    }

    mutate({
      poolName: initialPool.name,
      requestBody: {
        description: parsedDescription,
        include_deferred: editPoolRequestBody.include_deferred,
        pool: editPoolRequestBody.name,
        slots: editPoolRequestBody.slots,
      },
      updateMask,
    });
  };

  return { editPool, error, isPending, setError };
};
