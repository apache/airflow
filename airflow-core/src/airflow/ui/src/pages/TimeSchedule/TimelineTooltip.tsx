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
import { Separator, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { dayjs } from "./dateUtils";
import type { TimelineItem } from "./types";

type TimelineTooltipProps = {
  readonly item: TimelineItem;
  readonly selectedTimezone: string;
};

const formatTime = (datetime: string | null, selectedTimezone: string) =>
  datetime === null ? "—" : dayjs(datetime).tz(selectedTimezone).format("HH:mm");

export const TimelineTooltip = ({ item, selectedTimezone }: TimelineTooltipProps) => {
  const { t: translate } = useTranslation();
  const startTime = formatTime(item.startDate, selectedTimezone);
  const state = item.isPlanned ? "scheduled" : item.state;

  return (
    <VStack
      align="start"
      color="fg.inverted"
      data-testid="time-schedule-tooltip"
      gap={1}
      lineHeight="short"
      maxWidth="240px"
      p={2}
    >
      <Text fontSize="sm" fontWeight="semibold">
        {item.label}
      </Text>
      <Separator
        borderColor="currentColor"
        data-testid="time-schedule-tooltip-separator"
        my={1}
        opacity={0.2}
        width="100%"
      />
      <Text fontSize="xs" fontWeight="medium">
        {translate(`states.${state}`)}
      </Text>
      <Text fontSize="xs">
        {item.isPlanned
          ? translate("timeSchedule.nextRun", { time: startTime })
          : `${startTime} – ${formatTime(item.endDate, selectedTimezone)}`}
      </Text>
      {!item.isPlanned && !item.isPlaceholder ? (
        <Text fontSize="xs">{translate("timeSchedule.dagRuns", { count: item.runCount })}</Text>
      ) : undefined}
    </VStack>
  );
};
