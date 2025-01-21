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

import { useVariableServiceBulkVariables, useVariableServiceGetVariablesKey } from "openapi/queries";
import { toaster } from "src/components/ui";

export const useImportVariables = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async (responseData: { create?: { errors: Array<unknown>; success: Array<string> } }) => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    if (responseData.create) {
      const { errors, success } = responseData.create;

      if (Array.isArray(errors) && errors.length > 0) {
        const apiError = errors[0] as { error: string };

        setError({
          body: { detail: apiError.error },
        });
      } else if (Array.isArray(success) && success.length > 0) {
        toaster.create({
          description: `${success.length} variables created successfully. Keys: ${success.join(", ")}`,
          title: "Import Variables Request Successful",
          type: "success",
        });
        onSuccessConfirm();
      }
    }
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useVariableServiceBulkVariables({
    onError,
    onSuccess,
  });

  return { error, isPending, mutate, setError };
};
