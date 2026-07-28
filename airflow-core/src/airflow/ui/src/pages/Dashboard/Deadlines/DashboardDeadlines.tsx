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
import { Box, Flex, Heading } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useAutoRefresh } from "src/utils";

import { useDashboardMissedDeadlines, useDashboardPendingDeadlines } from "../useDashboardDeadlines";
import { DeadlineSection } from "./DeadlineSection";

type DashboardDeadlinesProps = {
  readonly endDate: string;
  readonly startDate: string;
};

const buildDeadlinesPath = (params: Record<string, string>) => {
  const searchParams = new URLSearchParams(params);

  return `/deadlines?${searchParams.toString()}`;
};

export const DashboardDeadlines = ({ endDate, startDate }: DashboardDeadlinesProps) => {
  const { t: translate } = useTranslation("dashboard");
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true });

  const {
    data: pendingData,
    error: pendingError,
    isLoading: isPendingLoading,
  } = useDashboardPendingDeadlines({ refetchInterval });
  const {
    data: missedData,
    error: missedError,
    isLoading: isMissedLoading,
  } = useDashboardMissedDeadlines({ endDate, refetchInterval, startDate });

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];

  const now = dayjs().toISOString();
  const pendingDeadlinesPath = buildDeadlinesPath({
    [SearchParamsKeys.DEADLINE_TIME_GTE]: now,
    [SearchParamsKeys.MISSED]: "false",
  });
  const missedDeadlinesPath = buildDeadlinesPath({
    [SearchParamsKeys.DEADLINE_TIME_GTE]: startDate,
    [SearchParamsKeys.DEADLINE_TIME_LTE]: endDate,
    [SearchParamsKeys.MISSED]: "true",
  });

  const hasNoDeadlines =
    !Boolean(pendingError) &&
    !Boolean(missedError) &&
    !isPendingLoading &&
    !isMissedLoading &&
    (pendingData?.total_entries ?? 0) === 0 &&
    (missedData?.total_entries ?? 0) === 0;

  if (hasNoDeadlines) {
    return undefined;
  }

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
          isLoading={isPendingLoading}
          showMoreLabel={translate("deadlines.showMore")}
          showMoreTo={pendingDeadlinesPath}
          title={translate("deadlines.pending.title")}
          totalEntries={pendingData?.total_entries ?? 0}
        />
        <DeadlineSection
          deadlines={missedDeadlines}
          emptyLabel={translate("deadlines.recentlyMissed.empty")}
          isLoading={isMissedLoading}
          showMoreLabel={translate("deadlines.showMore")}
          showMoreTo={missedDeadlinesPath}
          title={translate("deadlines.recentlyMissed.title")}
          totalEntries={missedData?.total_entries ?? 0}
        />
      </Flex>
    </Box>
  );
};
