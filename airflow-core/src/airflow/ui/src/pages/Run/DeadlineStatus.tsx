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
import { Badge, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiCheck, FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDagDeadlineAlerts, useDeadlinesServiceGetDeadlines } from "openapi/queries";
import Time from "src/components/Time";

type DeadlineStatusProps = {
  readonly dagId: string;
  readonly dagRunId: string;
};

export const DeadlineStatus = ({ dagId, dagRunId }: DeadlineStatusProps) => {
  const { t: translate } = useTranslation("dag");

  const { data: deadlineData, isLoading: isLoadingDeadlines } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId,
    limit: 10,
    orderBy: ["deadline_time"],
  });

  const { data: alertData, isLoading: isLoadingAlerts } = useDeadlinesServiceGetDagDeadlineAlerts({
    dagId,
  });

  if (isLoadingDeadlines || isLoadingAlerts) {
    return undefined;
  }

  const deadlines = deadlineData?.deadlines ?? [];
  const hasAlerts = (alertData?.total_entries ?? 0) > 0;

  if (deadlines.length === 0 && !hasAlerts) {
    return undefined;
  }

  if (deadlines.length === 0 && hasAlerts) {
    return (
      <HStack gap={1}>
        <Badge colorPalette="green" size="sm" variant="solid">
          <FiCheck color="var(--chakra-colors-fg-white)" />
          {translate("deadlineStatus.met")}
        </Badge>
      </HStack>
    );
  }

  const missedDeadlines = deadlines.filter((dl) => dl.missed);
  const upcomingDeadlines = deadlines.filter((dl) => !dl.missed);

  if (missedDeadlines.length > 0) {
    return (
      <VStack alignItems="flex-start" gap={1}>
        {missedDeadlines.map((dl) => (
          <HStack gap={1} key={dl.id}>
            <Badge colorPalette="red" size="sm" variant="solid">
              <FiAlertTriangle color="var(--chakra-colors-fg-white)" />
              {translate("deadlineStatus.missed")}
            </Badge>
            <Time datetime={dl.deadline_time} fontSize="sm" />
            {dl.alert_name !== undefined && dl.alert_name !== null && dl.alert_name !== "" ? (
              <Text color="fg.muted" fontSize="xs">
                ({dl.alert_name})
              </Text>
            ) : undefined}
          </HStack>
        ))}
      </VStack>
    );
  }

  return (
    <VStack alignItems="flex-start" gap={1}>
      {upcomingDeadlines.map((dl) => (
        <HStack gap={1} key={dl.id}>
          <Badge colorPalette="blue" size="sm" variant="solid">
            <FiClock color="var(--chakra-colors-fg-white)" />
            {translate("deadlineStatus.upcoming")}
          </Badge>
          <Time datetime={dl.deadline_time} fontSize="sm" />
          {dl.alert_name !== undefined && dl.alert_name !== null && dl.alert_name !== "" ? (
            <Text color="fg.muted" fontSize="xs">
              ({dl.alert_name})
            </Text>
          ) : undefined}
        </HStack>
      ))}
    </VStack>
  );
};
