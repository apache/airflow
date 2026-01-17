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
import { useMutation, useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import type { Dispatch, SetStateAction } from "react";
import { useTranslation } from "react-i18next";

import { ConnectionService } from "openapi/requests";
import type { ConnectionTestStatusResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

const POLL_INTERVAL = 1000;
const MAX_POLL_TIME_MS = 60_000;

export const useTestConnection = (setConnected: Dispatch<SetStateAction<boolean | undefined>>) => {
  const { t: translate } = useTranslation("admin");
  const [requestId, setRequestId] = useState<string | undefined>(undefined);
  const [isPolling, setIsPolling] = useState(false);
  const pollStartTimeRef = useRef<number | undefined>(undefined);

  const onQueueSuccess = useCallback(
    (res: { request_id: string }) => {
      setRequestId(res.request_id);
      setIsPolling(true);
      pollStartTimeRef.current = Date.now();
      toaster.create({
        description: translate("connections.testQueued.description"),
        title: translate("connections.testQueued.title"),
        type: "info",
      });
    },
    [translate],
  );

  const onQueueError = useCallback(() => {
    setConnected(false);
    setIsPolling(false);
    pollStartTimeRef.current = undefined;
    toaster.create({
      description: translate("connections.testError.description"),
      title: translate("connections.testError.title"),
      type: "error",
    });
  }, [setConnected, translate]);

  const queueMutation = useMutation({
    mutationFn: ConnectionService.testConnection,
    onError: onQueueError,
    onSuccess: onQueueSuccess,
  });

  const statusQuery = useQuery<ConnectionTestStatusResponse>({
    enabled: isPolling && requestId !== undefined,
    queryFn: () => ConnectionService.getConnectionTestStatus({ requestId: requestId as string }),
    queryKey: ["connectionTestStatus", requestId],
    refetchInterval: (query) => {
      const { data } = query.state;

      if (data?.state === "success" || data?.state === "failed") {
        return false;
      }

      if (
        pollStartTimeRef.current !== undefined &&
        Date.now() - pollStartTimeRef.current > MAX_POLL_TIME_MS
      ) {
        return false;
      }

      return POLL_INTERVAL;
    },
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (!statusQuery.data || !isPolling) {
      return;
    }

    const { data } = statusQuery;

    if (data.state === "success") {
      setIsPolling(false);
      pollStartTimeRef.current = undefined;
      setConnected(data.result_status ?? false);
      toaster.create({
        description: data.result_message ?? translate("connections.testSuccess.description"),
        title: translate("connections.testSuccess.title"),
        type: "success",
      });
    } else if (data.state === "failed") {
      setIsPolling(false);
      pollStartTimeRef.current = undefined;
      setConnected(false);
      toaster.create({
        description: data.result_message ?? translate("connections.testError.description"),
        title: translate("connections.testError.title"),
        type: "error",
      });
    } else if (
      pollStartTimeRef.current !== undefined &&
      Date.now() - pollStartTimeRef.current > MAX_POLL_TIME_MS
    ) {
      setIsPolling(false);
      pollStartTimeRef.current = undefined;
      setConnected(false);
      toaster.create({
        description: translate("connections.testTimeout.description"),
        title: translate("connections.testTimeout.title"),
        type: "error",
      });
    }
  }, [statusQuery, isPolling, setConnected, translate]);

  return {
    isPending: queueMutation.isPending || isPolling,
    mutate: queueMutation.mutate,
  };
};
