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

import { useConnectionServiceGetConnectionsKey, useConnectionServicePostConnection } from "openapi/queries";
import { toaster } from "src/components/ui";
import type { ConnectionBody } from "src/pages/Connections/Connections";

export const useAddConnection = ({ onSuccessConfirm }: { onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useConnectionServiceGetConnectionsKey],
    });

    toaster.create({
      description: "Connection has been added successfully",
      title: "Connection Add Request Submitted",
      type: "success",
    });

    setError(undefined);
    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useConnectionServicePostConnection({
    onError,
    onSuccess,
  });

  const addConnection = (requestBody: ConnectionBody) => {
    const description = requestBody.description === "" ? undefined : requestBody.description;
    const host = requestBody.host === "" ? undefined : requestBody.host;
    const login = requestBody.login === "" ? undefined : requestBody.login;
    const password = requestBody.password === "" ? undefined : requestBody.password;
    const port = requestBody.port === "" ? undefined : Number(requestBody.port);
    const schema = requestBody.schema === "" ? undefined : requestBody.schema;
    const extra = requestBody.extra === "{}" ? undefined : requestBody.extra;

    mutate({
      requestBody: {
        conn_type: requestBody.conn_type,
        connection_id: requestBody.connection_id,
        description,
        extra,
        host,
        login,
        password,
        port,
        schema,
      },
    });
  };

  return { addConnection, error, isPending };
};
