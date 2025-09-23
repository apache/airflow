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
import { Box, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { getDuration } from "src/utils";
import { formatDate } from "src/utils/datetimeUtils";

import type { GanttDataItem } from "./utils";

type GanttTooltipProps = {
  readonly data: Array<GanttDataItem>;
  readonly selectedTimezone: string;
  readonly taskId?: string;
  readonly x: number;
  readonly y: number;
};

export const GanttTooltip = ({ data, selectedTimezone, taskId, x, y }: GanttTooltipProps) => {
  const { t: translate } = useTranslation("common");

  if (taskId === undefined) {
    return undefined;
  }

  const taskInstance = data.find((dataItem) => dataItem.taskId === taskId);

  if (!taskInstance) {
    return undefined;
  }

  const startDate = formatDate(taskInstance.x[0], selectedTimezone);
  const endDate = formatDate(taskInstance.x[1], selectedTimezone);
  const duration = getDuration(taskInstance.x[0], taskInstance.x[1]);

  return (
    <Box
      bg={{ _dark: "white", base: "gray.900" }}
      borderColor={{ _dark: "gray.200", base: "gray.600" }}
      borderRadius="sm"
      boxShadow="xs"
      color={{ _dark: "gray.900", base: "white" }}
      left={`${x}px`}
      pointerEvents="none"
      position="fixed"
      px={2}
      py={1}
      top={`${y}px`}
      zIndex={100}
    >
      <VStack align="start" gap={1}>
        <Text fontSize="sm" fontWeight="semibold">
          {taskInstance.taskId}
        </Text>
        <Text fontSize="xs">
          {translate("state")}: {translate(`states.${taskInstance.state}`)}
        </Text>
        <Text fontSize="xs">
          {translate("startDate")}: {startDate}
        </Text>
        <Text fontSize="xs">
          {translate("endDate")}: {endDate}
        </Text>
        <Text fontSize="xs">
          {translate("duration")}: {duration}
        </Text>
      </VStack>
    </Box>
  );
};
