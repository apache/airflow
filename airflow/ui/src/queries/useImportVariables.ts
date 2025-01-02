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

import { useVariableServiceGetVariablesKey, useVariableServiceImportVariables } from "openapi/queries";
import { toaster } from "src/components/ui";

export const useImportVariables = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async (responseData: {
    created_count: number;
    created_variable_keys: Array<string>;
    import_count: number;
  }) => {
    await queryClient.invalidateQueries({
      queryKey: [useVariableServiceGetVariablesKey],
    });

    toaster.create({
      description: `${responseData.created_count} of ${responseData.import_count} variables imported successfully. Keys imported are ${responseData.created_variable_keys.join(", ")}`,
      title: "Import Variables Request Successful",
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useVariableServiceImportVariables({
    onError,
    onSuccess,
  });

  return { error, isPending, mutate, setError };
};
