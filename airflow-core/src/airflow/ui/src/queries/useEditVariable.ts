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
  const { t: translate } = useTranslation(["common", "admin"]);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    toaster.create({
      description: translate("toaster.update.success.description", {
        resourceName: translate("admin:variables.variable_one"),
      }),
      title: translate("toaster.update.success.title", {
        resourceName: translate("admin:variables.variable_one"),
      }),
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

  const editVariable = (editVariableRequestBody: VariableBody) => {
    const updateMask: Array<string> = [];

    if (editVariableRequestBody.value !== initialVariable.value) {
      updateMask.push("value");
    }
    if (editVariableRequestBody.description !== initialVariable.description) {
      updateMask.push("description");
    }
    if (editVariableRequestBody.team_name !== initialVariable.team_name) {
      updateMask.push("team_name");
    }

    const parsedDescription =
      editVariableRequestBody.description === "" ? undefined : editVariableRequestBody.description;

    mutate({
      requestBody: {
        description: parsedDescription,
        key: editVariableRequestBody.key,
        team_name: editVariableRequestBody.team_name,
        value: editVariableRequestBody.value,
      },
      updateMask,
      variableKey: initialVariable.key,
    });
  };

  return { editVariable, error, isPending, setError };
};
