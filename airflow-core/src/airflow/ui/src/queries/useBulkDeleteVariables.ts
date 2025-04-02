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

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export const useBulkDeleteVariables = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async (responseData: { delete?: { errors: Array<unknown>; success: Array<string> } }) => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    if (responseData.delete) {
      const { errors, success } = responseData.delete;

      if (Array.isArray(errors) && errors.length > 0) {
        const apiError = errors[0] as { error: string };

        setError({
          body: { detail: apiError.error },
        });
      } else if (Array.isArray(success) && success.length > 0) {
        toaster.create({
          description: `${success.length} variables deleted successfully. Keys: ${success.join(", ")}`,
          title: "Delete Variables Request Successful",
          type: "success",
        });
        clearSelections();
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

  return { error, isPending, mutate };
};
