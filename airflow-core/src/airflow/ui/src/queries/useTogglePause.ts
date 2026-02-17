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
  UseDagRunServiceGetDagRunsKeyFn,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  useDagServicePatchDag,
  useDagServiceGetDagsUiKey,
  UseTaskInstanceServiceGetTaskInstancesKeyFn,
} from "openapi/queries";
import { toaster } from "src/components/ui";

export const useTogglePause = ({ dagId }: { dagId: string }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("common");

  const onSuccess = async () => {
    const queryKeys = [
      [useDagServiceGetDagsUiKey],
      UseDagServiceGetDagKeyFn({ dagId }, [{ dagId }]),
      UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }]),
      UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
      UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));
  };

  const onError = (error: Error) => {
    // Get status from error
    const status =
      (error as unknown as { status?: number }).status ??
      (error as unknown as { response?: { status?: number } }).response?.status;

    // Skip 403 errors as they are handled by MutationCache
    if (status === 403) {
      return;
    }

    toaster.create({
      description: error.message,
      title: translate("error.title"),
      type: "error",
    });
  };

  return useDagServicePatchDag({
    onError,
    onSuccess,
  });
};
