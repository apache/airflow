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
import { Badge, Box, Flex, Heading, HStack, Link, Separator, Skeleton, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { useAutoRefresh } from "src/utils";

const LIMIT = 5;

const DeadlineRow = ({ deadline }: { readonly deadline: DeadlineResponse }) => (
  <HStack justifyContent="space-between" px={2} py={1.5} width="100%">
    <VStack alignItems="flex-start" gap={0}>
      <Link asChild color="fg.info" fontSize="sm" fontWeight="bold">
        <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
          {deadline.dag_run_id}
        </RouterLink>
      </Link>
      {deadline.alert_name !== undefined && deadline.alert_name !== null && deadline.alert_name !== "" ? (
        <Text color="fg.muted" fontSize="xs">
          {deadline.alert_name}
        </Text>
      ) : undefined}
    </VStack>
    <Time datetime={deadline.deadline_time} fontSize="sm" />
  </HStack>
);

export const DagDeadlines = ({ dagId }: { readonly dagId: string }) => {
  const { t: translate } = useTranslation("dag");
  const refetchInterval = useAutoRefresh({ dagId });
  const now = dayjs().toISOString();

  const {
    data: pendingData,
    error: pendingError,
    isLoading: isPendingLoading,
  } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: "~",
      deadlineTimeGte: now,
      limit: LIMIT,
      missed: false,
      orderBy: ["deadline_time"],
    },
    undefined,
    { refetchInterval },
  );

  const last24h = dayjs().subtract(24, "hour").toISOString();

  const {
    data: missedData,
    error: missedError,
    isLoading: isMissedLoading,
  } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: "~",
      lastUpdatedAtGte: last24h,
      limit: LIMIT,
      missed: true,
      orderBy: ["-last_updated_at"],
    },
    undefined,
    { refetchInterval },
  );

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];

  if (
    !isPendingLoading &&
    !isMissedLoading &&
    pendingDeadlines.length === 0 &&
    missedDeadlines.length === 0
  ) {
    return undefined;
  }

  return (
    <Box>
      <Flex color="fg.muted" mb={2}>
        <FiClock />
        <Heading ml={1} size="xs">
          {translate("overview.deadlines.title")}
        </Heading>
      </Flex>
      <ErrorAlert error={pendingError ?? missedError} />
      <Flex flexDirection={{ base: "column", md: "row" }} gap={{ base: 4, md: 8 }}>
        {isPendingLoading || pendingDeadlines.length > 0 ? (
          <Box borderRadius="lg" borderWidth={1} flex={1} overflow="hidden" p={3}>
            <HStack mb={2}>
              <FiClock />
              <Heading size="xs">{translate("overview.deadlines.pending")}</Heading>
              {pendingData ? (
                <Badge colorPalette="blue" size="sm" variant="solid">
                  {pendingData.total_entries}
                </Badge>
              ) : undefined}
            </HStack>
            {isPendingLoading ? (
              <VStack>
                {Array.from({ length: 3 }).map((_, idx) => (
                  // eslint-disable-next-line react/no-array-index-key
                  <Skeleton height="36px" key={idx} width="100%" />
                ))}
              </VStack>
            ) : (
              <VStack gap={0} separator={<Separator />}>
                {pendingDeadlines.map((dl) => (
                  <DeadlineRow deadline={dl} key={dl.id} />
                ))}
              </VStack>
            )}
          </Box>
        ) : undefined}

        {isMissedLoading || missedDeadlines.length > 0 ? (
          <Box borderRadius="lg" borderWidth={1} flex={1} overflow="hidden" p={3}>
            <HStack color="fg.error" mb={2}>
              <FiAlertTriangle />
              <Heading size="xs">{translate("overview.deadlines.recentlyMissed")}</Heading>
              {missedData ? (
                <Badge colorPalette="failed" size="sm" variant="solid">
                  {missedData.total_entries}
                </Badge>
              ) : undefined}
            </HStack>
            {isMissedLoading ? (
              <VStack>
                {Array.from({ length: 3 }).map((_, idx) => (
                  // eslint-disable-next-line react/no-array-index-key
                  <Skeleton height="36px" key={idx} width="100%" />
                ))}
              </VStack>
            ) : (
              <VStack gap={0} separator={<Separator />}>
                {missedDeadlines.map((dl) => (
                  <DeadlineRow deadline={dl} key={dl.id} />
                ))}
              </VStack>
            )}
          </Box>
        ) : undefined}
      </Flex>
    </Box>
  );
};
