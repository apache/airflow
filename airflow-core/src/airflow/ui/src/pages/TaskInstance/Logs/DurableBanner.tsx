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
import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";

import { useDurableExecutionServiceGetDurableExecution } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests";

import { Alert, Tooltip } from "src/system-components";

import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { isStatePending, useAutoRefresh } from "src/utils";
import { renderDuration } from "src/utils/datetimeUtils";

export const DurableBanner = ({
  taskInstance,
  tryNumber,
}: {
  readonly taskInstance?: Pick<
    TaskInstanceResponse,
    "dag_id" | "dag_run_id" | "end_date" | "map_index" | "state" | "task_id" | "try_number"
  >;
  readonly tryNumber?: number;
}) => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const coordinates = useTaskInstanceCoordinates();
  const refetchInterval = useAutoRefresh({ dagId: taskInstance?.dag_id });

  const { data: record } = useDurableExecutionServiceGetDurableExecution(
    {
      ...coordinates,
      dagId: taskInstance?.dag_id ?? "",
      dagRunId: taskInstance?.dag_run_id ?? "",
      mapIndex: taskInstance?.map_index ?? -1,
      taskId: taskInstance?.task_id ?? "",
    },
    undefined,
    {
      // A first try never reconnects or reuses a result, so only retries can show the banner.
      enabled: taskInstance !== undefined && (tryNumber ?? 0) > 1,
      // The final record lands just before the task ends, so fetch once more if the last fetch predates the end.
      refetchInterval: (query) =>
        (isStatePending(taskInstance?.state) ||
          dayjs(query.state.dataUpdatedAt).isBefore(taskInstance?.end_date)) &&
        refetchInterval,
    },
  );

  const entry = (record?.tries ?? []).find(
    (attempt) =>
      attempt.try_number === tryNumber &&
      (attempt.decision === "reconnected" || attempt.decision === "already_succeeded"),
  );

  if (entry === undefined) {
    return undefined;
  }

  // A deferred, timed-out or killed try never updates its "running" entry, so it is unknown once the try ends.
  const tryIsLive =
    tryNumber === taskInstance?.try_number &&
    (taskInstance?.state === "running" || taskInstance?.state === "deferred");
  const status = entry.status === "running" && !tryIsLive ? null : entry.status;
  const seconds = entry.saved_seconds;
  const externalId =
    typeof entry.external_id === "string" ? entry.external_id : JSON.stringify(entry.external_id);
  let detail = translate(`logs.durable.detail.${entry.decision}`);

  if (entry.decision === "reconnected") {
    if (status === "failed") {
      detail = translate("logs.durable.detail.reconnectedFailed");
    } else if (status === "success" || status === null) {
      detail =
        seconds === null
          ? translate("logs.durable.detail.reconnectedNoDuration")
          : translate("logs.durable.detail.reconnected", { duration: renderDuration(Math.round(seconds)) });
    } else {
      detail = translate("logs.durable.detail.reconnectedRunning");
    }
  }

  const jobDetails = (
    <VStack align="start" gap={0}>
      <Text>{translate("logs.durable.job.id", { id: externalId })}</Text>
      {entry.submitted_at === undefined || entry.submitted_at === null ? undefined : (
        <Text>
          {translate("logs.durable.job.submittedAt")} <Time datetime={entry.submitted_at} />
        </Text>
      )}
      {seconds === null ? undefined : (
        <Text>
          {translate("logs.durable.job.runTime", { duration: renderDuration(Math.round(seconds)) })}
        </Text>
      )}
    </VStack>
  );

  return (
    <Box mb={2}>
      <Alert status="info" title={translate(`logs.durable.title.${entry.decision}`)}>
        <HStack gap={2}>
          <Text>{detail}</Text>
          {status === null ? undefined : (
            <Tooltip content={jobDetails}>
              <StateBadge aria-label={translate(`common:states.${status}`)} state={status} />
            </Tooltip>
          )}
        </HStack>
      </Alert>
    </Box>
  );
};
