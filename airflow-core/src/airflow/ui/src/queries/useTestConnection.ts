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
import type { Dispatch, SetStateAction } from "react";

import { useConnectionServiceTestConnection, useConnectionServiceGetConnectionsKey } from "openapi/queries";
import type { ConnectionServiceTestConnectionMutationResult } from "openapi/queries/common";
import { toaster } from "src/components/ui";

type Error = {
  body: { detail: string };
  status: number;
};

export const useTestConnection = (setConnected: Dispatch<SetStateAction<boolean | undefined>>) => {
  const queryClient = useQueryClient();

  const onSuccess = async (res: ConnectionServiceTestConnectionMutationResult) => {
    await queryClient.invalidateQueries({
      queryKey: [useConnectionServiceGetConnectionsKey],
    });

    toaster.create({
      description: res.message,
      title: `Test Connection (200 - ${res.status})`,
      type: res.status ? "success" : "error",
    });
    setConnected(res.status);
  };

  const onError = (err: Error) => {
    toaster.create({
      description: err.body.detail,
      title: `Test Connection (${err.status})`,
      type: "error",
    });
    setConnected(false);
  };

  return useConnectionServiceTestConnection({
    onError,
    onSuccess,
  });
};
