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

import { useConnectionServiceDeleteConnection, useConnectionServiceGetConnectionsKey } from "openapi/queries";
import { toaster } from "src/components/ui";

export const useDeleteConnection = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["admin", "common"]);

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("common:toaster.delete.error", {
        resourceName: translate("admin:connections.connection_one"),
      }),
      type: "error",
    });
  };

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useConnectionServiceGetConnectionsKey],
    });

    toaster.create({
      description: translate("common:toaster.delete.success.description", {
        resourceName: translate("admin:connections.connection_one"),
      }),
      title: translate("common:toaster.delete.success.title", {
        resourceName: translate("admin:connections.connection_one"),
      }),
      type: "success",
    });

    onSuccessConfirm();
  };

  return useConnectionServiceDeleteConnection({
    onError,
    onSuccess,
  });
};
