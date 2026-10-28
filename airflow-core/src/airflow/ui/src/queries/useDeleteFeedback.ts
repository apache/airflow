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

import { toaster } from "src/system-components";

import { createErrorToaster } from "src/utils";

type Props = {
  onSuccessConfirm: () => void;
  queryKey: string;
  resourceName: "admin:connections.connection_one" | "admin:pools.pool_one" | "admin:variables.variable_one";
};

export const useDeleteFeedback = ({ onSuccessConfirm, queryKey, resourceName }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "admin"]);

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      {
        params: { resourceName: translate(resourceName) },
        titleKey: "common:toaster.delete.error",
      },
      translate,
    );
  };

  const onSuccess = async () => {
    await queryClient.invalidateQueries({ queryKey: [queryKey] });

    toaster.create({
      description: translate("common:toaster.delete.success.description", {
        resourceName: translate(resourceName),
      }),
      title: translate("common:toaster.delete.success.title", {
        resourceName: translate(resourceName),
      }),
      type: "success",
    });

    onSuccessConfirm();
  };

  return { onError, onSuccess };
};
