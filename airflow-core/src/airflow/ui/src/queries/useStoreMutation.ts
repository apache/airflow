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

import { toaster } from "src/components/ui";
import { createErrorToaster } from "src/utils";

type StoreMutationOperation = "create" | "delete" | "update";

type UseStoreMutationOptions = {
  readonly invalidationKeys: ReadonlyArray<unknown>;
  readonly onSuccessConfirm?: () => void;
  readonly operation: StoreMutationOperation;
  /** Already-translated, human readable name of the mutated resource. */
  readonly resourceName: string;
};

export const useStoreMutation = ({
  invalidationKeys,
  onSuccessConfirm,
  operation,
  resourceName,
}: UseStoreMutationOptions) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("common");

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      { params: { resourceName }, titleKey: `toaster.${operation}.error` },
      translate,
    );
  };

  const onSuccess = async () => {
    await Promise.all(
      invalidationKeys.map((queryKey) => queryClient.invalidateQueries({ queryKey: [queryKey] })),
    );

    onSuccessConfirm?.();

    toaster.create({
      description: translate(`toaster.${operation}.success.description`, { resourceName }),
      title: translate(`toaster.${operation}.success.title`, { resourceName }),
      type: "success",
    });
  };

  return { onError, onSuccess };
};
