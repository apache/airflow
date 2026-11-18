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
    "dag_id" | "dag_run_id" | "map_index" | "state" | "task_id"
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
      refetchInterval: isStatePending(taskInstance?.state) && refetchInterval,
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

  const seconds = entry.saved_seconds;
  const externalId =
    typeof entry.external_id === "string" ? entry.external_id : JSON.stringify(entry.external_id);
  let detail = translate(`logs.durable.detail.${entry.decision}`);

  if (entry.decision === "reconnected") {
    detail =
      seconds === null
        ? translate("logs.durable.detail.reconnectedRunning")
        : translate("logs.durable.detail.reconnected", { duration: renderDuration(Math.round(seconds)) });
  }

  const jobDetails = (
    <VStack align="start" gap={0}>
      <Text>{translate("logs.durable.job.id", { id: externalId })}</Text>
      {record?.submitted_at === undefined || record.submitted_at === null ? undefined : (
        <Text>
          {translate("logs.durable.job.submittedAt")} <Time datetime={record.submitted_at} />
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
          <Tooltip content={jobDetails}>
            <StateBadge state={entry.status}>
              {translate("logs.durable.job.label")}
              {entry.status === null ? undefined : `: ${translate(`common:states.${entry.status}`)}`}
            </StateBadge>
          </Tooltip>
        </HStack>
      </Alert>
    </Box>
  );
};
