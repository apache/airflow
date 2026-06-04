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
import { Box, Flex, Heading, HStack, Link, Separator, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { DeadlineResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useAutoRefresh } from "src/utils";

import {
  getDeadlineWindow,
  useDashboardMissedDeadlines,
  useDashboardPendingDeadlines,
} from "../useDashboardDeadlines";

const buildDeadlinesPath = (params: Record<string, string>) => {
  const searchParams = new URLSearchParams(params);

  return `/deadlines?${searchParams.toString()}`;
};

type DeadlineItemProps = {
  readonly deadline: DeadlineResponse;
};

const DeadlineItem = ({ deadline }: DeadlineItemProps) => (
  <HStack justify="space-between" px={3} py={2} width="100%">
    <VStack align="start" gap={0} minWidth={0} overflow="hidden">
      <Link asChild color="fg.info" fontSize="sm" fontWeight="medium">
        <RouterLink to={`/dags/${deadline.dag_id}`}>
          <TruncatedText text={deadline.dag_id} />
        </RouterLink>
      </Link>
      <Link asChild color="fg.muted" fontSize="xs">
        <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
          <TruncatedText text={deadline.dag_run_id} />
        </RouterLink>
      </Link>
    </VStack>
    <Box color="fg.muted" flexShrink={0} fontSize="xs">
      <Time datetime={deadline.deadline_time} />
    </Box>
  </HStack>
);

type DeadlineSectionProps = {
  readonly deadlines: Array<DeadlineResponse>;
  readonly emptyLabel: string;
  readonly showMoreLabel: string;
  readonly showMoreTo: string;
  readonly subtitle?: string;
  readonly title: string;
  readonly totalEntries: number;
};

const DeadlineSection = ({
  deadlines,
  emptyLabel,
  showMoreLabel,
  showMoreTo,
  subtitle,
  title,
  totalEntries,
}: DeadlineSectionProps) => {
  const hasMoreDeadlines = totalEntries > deadlines.length;

  return (
    <Box flex={1} minWidth={0}>
      <Box borderRadius="md" borderWidth="1px" overflow="hidden">
        <HStack justify="space-between" px={3} py={2}>
          <Heading color="fg.muted" size="xs">
            {title}
          </Heading>
          {subtitle === undefined ? undefined : (
            <Text color="fg.muted" fontSize="xs">
              {subtitle}
            </Text>
          )}
        </HStack>
        <Separator />
        {deadlines.length === 0 ? (
          <Text color="fg.muted" fontSize="sm" px={3} py={3} textAlign="center">
            {emptyLabel}
          </Text>
        ) : (
          <VStack align="stretch" gap={0} separator={<Separator />}>
            {deadlines.map((deadline) => (
              <DeadlineItem deadline={deadline} key={deadline.id} />
            ))}
          </VStack>
        )}
        {hasMoreDeadlines ? (
          <>
            <Separator />
            <Flex justify="flex-end" px={3} py={2}>
              <Link asChild color="fg.info" fontSize="xs" fontWeight="medium">
                <RouterLink to={showMoreTo}>{showMoreLabel}</RouterLink>
              </Link>
            </Flex>
          </>
        ) : undefined}
      </Box>
    </Box>
  );
};

export const DashboardDeadlines = () => {
  const { t: translate } = useTranslation("dashboard");
  const deadlineWindow = getDeadlineWindow();
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true });

  const { data: pendingData, error: pendingError } = useDashboardPendingDeadlines({ refetchInterval });
  const { data: missedData, error: missedError } = useDashboardMissedDeadlines({ refetchInterval });

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];
  const pendingDeadlinesPath = buildDeadlinesPath({
    [SearchParamsKeys.DEADLINE_TIME_GTE]: deadlineWindow.now,
    [SearchParamsKeys.MISSED]: "false",
  });
  const missedDeadlinesPath = buildDeadlinesPath({
    [SearchParamsKeys.DEADLINE_TIME_GTE]: deadlineWindow.lastDay,
    [SearchParamsKeys.DEADLINE_TIME_LTE]: deadlineWindow.now,
    [SearchParamsKeys.MISSED]: "true",
  });

  return (
    <Box>
      <Flex alignItems="center" color="fg.muted" my={2}>
        <FiClock />
        <Heading ml={1} size="xs">
          {translate("deadlines.title")}
        </Heading>
      </Flex>
      <ErrorAlert error={pendingError ?? missedError} />
      <Flex flexDirection={{ base: "column", md: "row" }} gap={4}>
        <DeadlineSection
          deadlines={pendingDeadlines}
          emptyLabel={translate("deadlines.pending.empty")}
          showMoreLabel={translate("deadlines.showMore")}
          showMoreTo={pendingDeadlinesPath}
          title={translate("deadlines.pending.title")}
          totalEntries={pendingData?.total_entries ?? 0}
        />
        <DeadlineSection
          deadlines={missedDeadlines}
          emptyLabel={translate("deadlines.recentlyMissed.empty")}
          showMoreLabel={translate("deadlines.showMore")}
          showMoreTo={missedDeadlinesPath}
          subtitle={translate("deadlines.recentlyMissed.subtitle")}
          title={translate("deadlines.recentlyMissed.title")}
          totalEntries={missedData?.total_entries ?? 0}
        />
      </Flex>
    </Box>
  );
};
