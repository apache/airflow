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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useTaskStateStoreServiceGetTaskStateStore } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests";

import { Alert } from "src/system-components";

import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { isStatePending, useAutoRefresh } from "src/utils";
import { renderDuration } from "src/utils/datetimeUtils";

// Written by ResumableJobMixin in the Task SDK.
const DURABLE_RECORD_KEY = "__airflow_durable__";

type DurableTry = {
  readonly decision: string;
  readonly external_id: unknown;
  readonly saved_seconds: number | null;
  readonly try_number: number | null;
};

const DECISIONS_THAT_SAVE = new Set(["already_succeeded", "reconnected"]);

export const DurableBanner = ({
  taskInstance,
  tryNumber,
}: {
  readonly taskInstance?: Pick<
    TaskInstanceResponse,
    "dag_id" | "dag_run_id" | "map_index" | "state" | "task_id"
  >;
  readonly tryNumber?: number;
}) => {
  const { t: translate } = useTranslation("dag");
  const coordinates = useTaskInstanceCoordinates();
  const refetchInterval = useAutoRefresh({ dagId: taskInstance?.dag_id });

  const { data } = useTaskStateStoreServiceGetTaskStateStore(
    {
      ...coordinates,
      dagId: taskInstance?.dag_id ?? "",
      dagRunId: taskInstance?.dag_run_id ?? "",
      key: DURABLE_RECORD_KEY,
      mapIndex: taskInstance?.map_index ?? -1,
      taskId: taskInstance?.task_id ?? "",
    },
    undefined,
    {
      enabled: taskInstance !== undefined,
      refetchInterval: isStatePending(taskInstance?.state) && refetchInterval,
      // Most tasks are not durable, so the key is usually missing.
      retry: false,
    },
  );

  const tries = (data?.value as { tries?: Array<DurableTry> } | undefined)?.tries ?? [];
  const entry = tries.find(
    (attempt) => attempt.try_number === tryNumber && DECISIONS_THAT_SAVE.has(attempt.decision),
  );

  if (entry === undefined) {
    return undefined;
  }

  const seconds = entry.saved_seconds;
  const externalId =
    typeof entry.external_id === "string" ? entry.external_id : JSON.stringify(entry.external_id);
  let detail = translate(`logs.durable.detail.${entry.decision}`, { id: externalId });

  if (entry.decision === "reconnected") {
    detail =
      seconds === null
        ? translate("logs.durable.detail.reconnectedRunning", { id: externalId })
        : translate("logs.durable.detail.reconnected", {
            duration: renderDuration(Math.round(seconds)),
            id: externalId,
          });
  }

  return (
    <Box mb={2}>
      <Alert status="info" title={translate(`logs.durable.title.${entry.decision}`)}>
        {detail}
      </Alert>
    </Box>
  );
};
