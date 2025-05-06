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

import { useVariableServiceGetVariablesKey, useVariableServicePostVariable } from "openapi/queries";
import { toaster } from "src/components/ui";
import type { VariableBody } from "src/pages/Variables/ManageVariable/VariableForm";

export const useAddVariable = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    toaster.create({
      description: "Variable has been added successfully",
      title: "Variable Add Request Submitted",
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useVariableServicePostVariable({
    onError,
    onSuccess,
  });

  const addVariable = (variableRequestBody: VariableBody) => {
    const parsedDescription =
      variableRequestBody.description === "" ? undefined : variableRequestBody.description;

    mutate({
      requestBody: {
        description: parsedDescription,
        key: variableRequestBody.key,
        value: variableRequestBody.value,
      },
    });
  };

  return { addVariable, error, isPending, setError };
};
