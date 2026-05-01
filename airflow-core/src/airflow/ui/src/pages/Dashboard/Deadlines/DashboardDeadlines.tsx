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
import { Badge, Box, Flex, Heading, HStack, Link, Separator, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys } from "src/constants/searchParams";

const LIMIT = 5;

type DeadlineItemProps = {
  readonly deadline: DeadlineResponse;
};

const DeadlineItem = ({ deadline }: DeadlineItemProps) => {
  const { t: translate } = useTranslation("dag");

  return (
    <HStack justify="space-between" px={3} py={2} width="100%">
      <HStack gap={2} minWidth={0} overflow="hidden">
        <Badge colorPalette={deadline.missed ? "red" : "blue"} flexShrink={0} size="sm" variant="solid">
          {deadline.missed ? <FiAlertTriangle /> : <FiClock />}
          {translate(deadline.missed ? "deadlineStatus.missed" : "deadlineStatus.upcoming")}
        </Badge>
        <VStack align="start" gap={0} minWidth={0} overflow="hidden">
          <Link asChild color="fg.info" fontSize="sm" fontWeight="medium">
            <RouterLink to={`/dags/${deadline.dag_id}`}>{deadline.dag_id}</RouterLink>
          </Link>
          <Link asChild color="fg.muted" fontSize="xs">
            <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
              <TruncatedText text={deadline.dag_run_id} />
            </RouterLink>
          </Link>
        </VStack>
      </HStack>
      <Time color="fg.muted" datetime={deadline.deadline_time} flexShrink={0} fontSize="xs" />
    </HStack>
  );
};

export const DashboardDeadlines = () => {
  const { t: translate } = useTranslation("dashboard");

  // Truncate to minute to avoid changing the query key on every render
  const yesterday = dayjs().subtract(24, "hour").startOf("minute").toISOString();

  const { data } = useDeadlinesServiceGetDeadlines({
    dagId: "~",
    dagRunId: "~",
    deadlineTimeGte: yesterday,
    limit: LIMIT,
    // missed descending → missed (true) before upcoming (false), then soonest deadline first
    orderBy: ["-missed", "deadline_time"],
  });

  const deadlines = data?.deadlines ?? [];
  const total = data?.total_entries ?? 0;

  if (deadlines.length === 0) {
    return undefined;
  }

  const seeAllHref = `/deadlines?${SearchParamsKeys.DEADLINE_TIME_GTE}=${yesterday}`;

  return (
    <Box>
      <Flex alignItems="center" color="fg.muted" my={2}>
        <FiClock />
        <Heading ml={1} size="xs">
          {translate("deadlines.title")}
        </Heading>
      </Flex>
      <Box borderRadius="md" borderWidth="1px" overflow="hidden">
        <HStack justify="space-between" px={3} py={2}>
          <Text color="fg.muted" fontSize="xs">
            {translate("deadlines.last24h")}
          </Text>
          {total > LIMIT ? (
            <Link asChild color="fg.info" fontSize="xs">
              <RouterLink to={seeAllHref}>{translate("deadlines.seeAll")}</RouterLink>
            </Link>
          ) : undefined}
        </HStack>
        <Separator />
        <VStack align="stretch" gap={0} separator={<Separator />}>
          {deadlines.map((deadline) => (
            <DeadlineItem deadline={deadline} key={deadline.id} />
          ))}
        </VStack>
      </Box>
    </Box>
  );
};
