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
import { Badge, Box, Heading, HStack, Link, Separator, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

const DeadlineRow = ({
  deadline,
  translate,
}: {
  readonly deadline: DeadlineResponse;
  readonly translate: (key: string) => string;
}) => (
  <HStack justifyContent="space-between" px={2} py={1.5} width="100%">
    <VStack alignItems="flex-start" gap={0}>
      <HStack>
        {deadline.missed ? (
          <FiAlertTriangle color="var(--chakra-colors-fg-error)" size={12} />
        ) : (
          <FiClock color="var(--chakra-colors-fg-info)" size={12} />
        )}
        <Badge colorPalette={deadline.missed ? "red" : "blue"} fontSize="2xs" size="sm" variant="solid">
          {deadline.missed ? translate("calendar.deadlines.missed") : translate("calendar.deadlines.pending")}
        </Badge>
        <Link asChild color="fg.info" fontSize="sm">
          <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
            {deadline.dag_run_id}
          </RouterLink>
        </Link>
      </HStack>
      {deadline.alert_name !== undefined && deadline.alert_name !== null && deadline.alert_name !== "" ? (
        <Text color="fg.muted" fontSize="xs" pl={5}>
          {deadline.alert_name}
        </Text>
      ) : undefined}
    </VStack>
    <Time datetime={deadline.deadline_time} fontSize="sm" />
  </HStack>
);

type CalendarDeadlinesProps = {
  readonly dagId: string;
  readonly endDate: string;
  readonly startDate: string;
};

export const CalendarDeadlines = ({ dagId, endDate, startDate }: CalendarDeadlinesProps) => {
  const { t: translate } = useTranslation("dag");

  const { data: pendingData } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId: "~",
    deadlineTimeGte: startDate,
    deadlineTimeLte: endDate,
    limit: 20,
    missed: false,
    orderBy: ["deadline_time"],
  });

  const { data: missedData } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId: "~",
    deadlineTimeGte: startDate,
    deadlineTimeLte: endDate,
    limit: 20,
    missed: true,
    orderBy: ["-deadline_time"],
  });

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];
  const allDeadlines = [...missedDeadlines, ...pendingDeadlines];

  if (allDeadlines.length === 0) {
    return undefined;
  }

  return (
    <Box borderRadius="lg" borderWidth={1} mt={6} p={4}>
      <HStack mb={3}>
        <FiClock />
        <Heading size="xs">{translate("calendar.deadlines.title")}</Heading>
        {missedDeadlines.length > 0 ? (
          <Badge colorPalette="red" size="sm" variant="solid">
            {missedDeadlines.length} {translate("calendar.deadlines.missed")}
          </Badge>
        ) : undefined}
        {pendingDeadlines.length > 0 ? (
          <Badge colorPalette="blue" size="sm" variant="solid">
            {pendingDeadlines.length} {translate("calendar.deadlines.pending")}
          </Badge>
        ) : undefined}
      </HStack>
      <VStack gap={0} separator={<Separator />}>
        {allDeadlines.map((dl) => (
          <DeadlineRow deadline={dl} key={dl.id} translate={translate} />
        ))}
      </VStack>
    </Box>
  );
};
