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
import { Link as RouterLink } from "react-router-dom";

import type { DagScheduleOverviewEntry } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";

import {
  HOUR_TICKS,
  formatScheduleBar,
  formatSecondsAsClock,
  formatTooltipBody,
} from "./scheduleOverviewUtils";

type Props = {
  readonly entry: DagScheduleOverviewEntry;
};

const ROW_HEIGHT_PX = 36;
const BAR_HEIGHT_PX = 14;

export const ScheduleOverviewRow = ({ entry }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const hasData = entry.recent_runs_count > 0;
  const bar = formatScheduleBar(entry.start_mean_seconds, entry.end_mean_seconds);
  const tooltipBody = formatTooltipBody(entry);

  return (
    <HStack
      alignItems="center"
      borderBottomWidth={1}
      borderColor="border.emphasized"
      data-testid={`schedule-overview-row-${entry.dag_id}`}
      gap={4}
      minH={`${ROW_HEIGHT_PX}px`}
      px={2}
      py={1}
    >
      <Box flex="0 0 280px" minW="200px">
        <RouterLink
          style={{ color: "var(--chakra-colors-brand-500)", fontWeight: 600 }}
          to={`/dags/${entry.dag_id}`}
        >
          {entry.dag_display_name}
        </RouterLink>
        <Text color="fg.muted" fontSize="xs">
          {hasData
            ? translate("scheduleOverview.recentRuns", { count: entry.recent_runs_count })
            : translate("scheduleOverview.noRecentRuns")}
        </Text>
      </Box>
      <Box flex="1 1 auto" position="relative">
        <Box
          // Tick rail: vertical guides at every 3 hours.
          alignItems="stretch"
          display="flex"
          height={`${BAR_HEIGHT_PX + 8}px`}
          justifyContent="space-between"
        >
          {HOUR_TICKS.map((hour) => (
            <Box
              borderLeftColor="border.subtle"
              borderLeftWidth={hour === 0 || hour === 24 ? 0 : 1}
              height="100%"
              key={hour}
              position="relative"
            >
              <Text color="fg.muted" fontSize="2xs" left="-14px" position="absolute" top="-2px" width="28px">
                {String(hour).padStart(2, "0")}:00
              </Text>
            </Box>
          ))}
        </Box>
        {hasData ? (
          <Tooltip content={<Box whiteSpace="pre-line">{tooltipBody}</Box>}>
            <Box
              _hover={{ bg: "brand.muted" }}
              bg="brand.500"
              borderRadius="sm"
              bottom="4px"
              data-testid={`schedule-overview-bar-${entry.dag_id}`}
              height={`${BAR_HEIGHT_PX}px`}
              left={`${bar.leftPercent}%`}
              position="absolute"
              title={tooltipBody}
              width={`${bar.widthPercent}%`}
            />
          </Tooltip>
        ) : (
          <Box
            color="fg.muted"
            data-testid={`schedule-overview-empty-${entry.dag_id}`}
            fontSize="xs"
            position="absolute"
            top={`${BAR_HEIGHT_PX}px`}
          >
            {translate("scheduleOverview.noData")}
          </Box>
        )}
      </Box>
    </HStack>
  );
};

type HeaderProps = {
  readonly labelColumnWidth: number;
  readonly totalEntries: number;
};

export const ScheduleOverviewHeader = ({ labelColumnWidth, totalEntries }: HeaderProps) => {
  const { t: translate } = useTranslation("dags");

  return (
    <VStack alignItems="stretch" mb={2}>
      <HStack borderBottomWidth={1} borderColor="border" pb={1}>
        <Box flex={`0 0 ${labelColumnWidth}px`} minW="200px">
          <Text fontSize="sm" fontWeight="bold">
            {translate("scheduleOverview.dagColumn")}
          </Text>
        </Box>
        <Box flex="1 1 auto">
          <Text fontSize="sm" fontWeight="bold">
            {translate("scheduleOverview.timelineColumn")}
          </Text>
        </Box>
      </HStack>
      <Text color="fg.muted" fontSize="xs">
        {translate("scheduleOverview.subtitle", { count: totalEntries })}
        {" · "}
        <Text as="span" color="fg.muted" fontSize="2xs">
          {translate("scheduleOverview.timelineRange", {
            end: formatSecondsAsClock(24 * 3600 - 1),
            start: formatSecondsAsClock(0),
          })}
        </Text>
      </Text>
    </VStack>
  );
};
