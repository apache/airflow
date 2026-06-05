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
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useConnectionServiceEnqueueConnectionTest,
  useConnectionServiceGetConnectionTest,
} from "openapi/queries";
import type { ApiError } from "openapi/requests";
import type { ConnectionTestRequestBody } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

type ConnectionStatus = boolean | undefined;

const ACTIVE_STATES = new Set(["pending", "queued", "running"]);
const POLL_INTERVAL = 2000;

const isActive = (state: string | undefined) => state !== undefined && ACTIVE_STATES.has(state);

export const useTestConnection = (setConnected: (status: ConnectionStatus) => void) => {
  const { t: translate } = useTranslation("admin");
  const [token, setToken] = useState<string | undefined>(undefined);

  const enqueue = useConnectionServiceEnqueueConnectionTest({
    onError: (error) => {
      setConnected(false);
      toaster.create({
        title:
          (error as ApiError).status === 409
            ? translate("connections.testInProgress.title")
            : translate("connections.testError.title"),
        type: "error",
      });
    },
    onSuccess: (response) => {
      setToken(response.token);
    },
  });

  const { data } = useConnectionServiceGetConnectionTest(
    { airflowConnectionTestToken: token ?? "" },
    undefined,
    {
      enabled: token !== undefined,
      refetchInterval: (query) => (isActive(query.state.data?.state) ? POLL_INTERVAL : false),
    },
  );

  const state = data?.state;
  const resultMessage = data?.result_message;

  useEffect(() => {
    if (state === undefined || isActive(state)) {
      return;
    }

    const succeeded = state === "success";

    setConnected(succeeded);
    toaster.create({
      description: resultMessage ?? undefined,
      title: succeeded
        ? translate("connections.testSuccess.title")
        : translate("connections.testError.title"),
      type: succeeded ? "success" : "error",
    });

    setToken(undefined);
  }, [state, resultMessage, setConnected, translate]);

  const test = (requestBody: ConnectionTestRequestBody) => {
    setConnected(undefined);
    enqueue.mutate({ requestBody });
  };

  const isPending = enqueue.isPending || (token !== undefined && (state === undefined || isActive(state)));

  return { isPending, test };
};
