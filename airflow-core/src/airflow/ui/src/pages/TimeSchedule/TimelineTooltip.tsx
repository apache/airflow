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
import { HStack, Separator, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { StateIcon } from "src/components/StateIcon";

import { useDurationFormat } from "src/utils";
import { formatDate } from "src/utils/datetimeUtils";

import { getTimelineItemIconState } from "./timelineUtils";

type TimelineTooltipProps = {
  readonly item: TimeScheduleItem;
  readonly selectedTimezone: string;
};

const formatTime = (datetime: string | null, selectedTimezone: string) =>
  datetime === null ? "—" : formatDate(datetime, selectedTimezone, "HH:mm");

export const TimelineTooltip = ({ item, selectedTimezone }: TimelineTooltipProps) => {
  const { t: translate } = useTranslation();
  const { renderDuration } = useDurationFormat();
  const startTime = formatTime(item.start_date, selectedTimezone);
  const iconState = getTimelineItemIconState(item);
  const state = iconState ?? item.state;

  return (
    <VStack align="start" data-testid="time-schedule-tooltip" gap={1} lineHeight="short" maxWidth="xs">
      <Text fontSize="sm" fontWeight="semibold">
        {item.dag_display_name}
      </Text>
      <Separator
        borderColor="currentColor"
        data-testid="time-schedule-tooltip-separator"
        my={1}
        opacity={0.2}
        width="100%"
      />
      <HStack gap={1}>
        <StateIcon color="currentColor" size={12} state={iconState} />
        <Text fontSize="xs" fontWeight="medium">
          {translate(`states.${state}`)}
        </Text>
      </HStack>
      <Text fontSize="xs">
        {item.is_planned
          ? `${translate("dagDetails.nextRun")}: ${startTime}`
          : `${startTime} – ${formatTime(item.end_date, selectedTimezone)}`}
      </Text>
      {!item.is_planned && !item.is_placeholder ? (
        <Text fontSize="xs">
          {item.run_count} {translate("dagRun", { count: item.run_count })}
        </Text>
      ) : undefined}
      {item.duration_ms > 0 ? <Text fontSize="xs">{renderDuration(item.duration_ms / 1000)}</Text> : null}
    </VStack>
  );
};
