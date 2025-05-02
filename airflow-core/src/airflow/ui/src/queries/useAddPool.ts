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

import { usePoolServiceGetPoolsKey, usePoolServicePostPool } from "openapi/queries";
import { toaster } from "src/components/ui";
import type { PoolBody } from "src/pages/Pools/PoolForm";

export const useAddPool = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [usePoolServiceGetPoolsKey],
    });

    toaster.create({
      description: "Pool has been added successfully",
      title: "Pool Add Request Submitted",
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = usePoolServicePostPool({
    onError,
    onSuccess,
  });

  const addPool = (poolRequestBody: PoolBody) => {
    const parsedDescription = poolRequestBody.description === "" ? undefined : poolRequestBody.description;

    mutate({
      requestBody: {
        description: parsedDescription,
        include_deferred: poolRequestBody.include_deferred,
        name: poolRequestBody.name,
        slots: poolRequestBody.slots,
      },
    });
  };

  return { addPool, error, isPending, setError };
};
