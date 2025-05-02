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

import { useVariableServiceGetVariablesKey, useVariableServicePatchVariable } from "openapi/queries";
import { toaster } from "src/components/ui";
import type { VariableBody } from "src/pages/Variables/ManageVariable/VariableForm";

export const useEditVariable = (
  initialVariable: VariableBody,
  {
    onSuccessConfirm,
  }: {
    onSuccessConfirm: () => void;
  },
) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    toaster.create({
      description: "Variable has been edited successfully",
      title: "Variable Edit Request Submitted",
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useVariableServicePatchVariable({
    onError,
    onSuccess,
  });

  const editVariable = (addVariableRequestBody: VariableBody) => {
    const updateMask: Array<string> = [];

    if (addVariableRequestBody.value !== initialVariable.value) {
      updateMask.push("value");
    }
    if (addVariableRequestBody.description !== initialVariable.description) {
      updateMask.push("description");
    }

    const parsedDescription =
      addVariableRequestBody.description === "" ? undefined : addVariableRequestBody.description;

    mutate({
      requestBody: {
        description: parsedDescription,
        key: addVariableRequestBody.key,
        value: addVariableRequestBody.value,
      },
      updateMask,
      variableKey: initialVariable.key,
    });
  };

  return { editVariable, error, isPending, setError };
};
