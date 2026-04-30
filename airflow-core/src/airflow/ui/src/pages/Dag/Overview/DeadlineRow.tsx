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
import { Badge, HStack, Link, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { DeadlineAlertResponse, DeadlineResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

dayjs.extend(duration);
dayjs.extend(relativeTime);

type DeadlineRowProps = {
  readonly alert?: DeadlineAlertResponse;
  readonly deadline: DeadlineResponse;
};

export const DeadlineRow = ({ alert, deadline }: DeadlineRowProps) => {
  const { t: translate } = useTranslation("dag");

  const reference = alert
    ? translate(`deadlineAlerts.referenceType.${alert.reference_type}`, {
        defaultValue: alert.reference_type,
      })
    : undefined;
  const interval = alert ? dayjs.duration(alert.interval, "seconds").humanize() : undefined;

  return (
    <HStack justifyContent="space-between" px={2} py={1.5} width="100%">
      <VStack alignItems="flex-start" gap={0}>
        <HStack gap={2}>
          <Badge colorPalette={deadline.missed ? "red" : "blue"} size="sm" variant="solid">
            {deadline.missed ? <FiAlertTriangle /> : <FiClock />}
            {translate(deadline.missed ? "deadlineStatus.missed" : "deadlineStatus.upcoming")}
          </Badge>
          <Link asChild color="fg.info" fontSize="sm" fontWeight="bold">
            <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
              {deadline.dag_run_id}
            </RouterLink>
          </Link>
        </HStack>
        {reference !== undefined && interval !== undefined ? (
          <Text color="fg.muted" fontSize="xs">
            {translate("deadlineAlerts.completionRule", { interval, reference })}
          </Text>
        ) : undefined}
      </VStack>
      <HStack gap={1}>
        <Text color="fg.muted" fontSize="xs">
          {translate("deadlineStatus.expected")}:
        </Text>
        <Time datetime={deadline.deadline_time} fontSize="xs" />
      </HStack>
    </HStack>
  );
};
