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

import { useConnectionServiceGetConnectionsKey, useConnectionServicePatchConnection } from "openapi/queries";
import { toaster } from "src/components/ui";
import type { ConnectionBody } from "src/pages/Connections/Connections";

export const useEditConnection = (
  initialConnection: ConnectionBody,
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
      queryKey: [useConnectionServiceGetConnectionsKey],
    });

    toaster.create({
      description: "Connection has been edited successfully",
      title: "Connection Edit Request Submitted",
      type: "success",
    });

    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useConnectionServicePatchConnection({
    onError,
    onSuccess,
  });

  const editConnection = (requestBody: ConnectionBody) => {
    const updateMask: Array<string> = [];

    if (requestBody.extra !== initialConnection.extra) {
      updateMask.push("extra");
    }
    if (requestBody.conn_type !== initialConnection.conn_type) {
      updateMask.push("conn_type");
    }
    if (requestBody.description !== initialConnection.description) {
      updateMask.push("description");
    }
    if (requestBody.host !== initialConnection.host) {
      updateMask.push("host");
    }
    if (requestBody.login !== initialConnection.login) {
      updateMask.push("login");
    }
    if (requestBody.password !== initialConnection.password) {
      updateMask.push("password");
    }
    if (requestBody.port !== initialConnection.port) {
      updateMask.push("port");
    }
    if (requestBody.schema !== initialConnection.schema) {
      updateMask.push("schema");
    }

    mutate({
      connectionId: initialConnection.connection_id,
      requestBody: {
        ...requestBody,
        conn_type: requestBody.conn_type,
        connection_id: initialConnection.connection_id,
        extra: requestBody.extra === "{}" ? undefined : requestBody.extra,
        // eslint-disable-next-line unicorn/no-null
        port: requestBody.port === "" ? null : Number(requestBody.port),
      },
      updateMask,
    });
  };

  return { editConnection, error, isPending, setError };
};
