/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Box, HStack, Text, VStack } from "@chakra-ui/react";

import type { CalendarCellData } from "./types";
import { createTooltipContent } from "./calendarUtils";

const SQUARE_SIZE = "12px";
const SQUARE_BORDER_RADIUS = "2px";

type Props = {
  readonly cellData: CalendarCellData;
};

const STATE_COLOR_MAP: Record<string, string> = {
  failed: "failed.solid",
  planned: "scheduled.solid",
  queued: "queued.solid",
  running: "running.solid",
  success: "success.solid",
} as const;

export const CalendarTooltipContent = ({ cellData }: Props) => {
  const { date, hasRuns, states, total } = createTooltipContent(cellData);

  if (!hasRuns) {
    return <Text fontSize="sm">{date}: No runs</Text>;
  }

  return (
    <VStack align="start" gap={2}>
      <Text fontSize="sm" fontWeight="medium">
        {date}: {total} runs
      </Text>
      <VStack align="start" gap={1.5}>
        {states.map(({ count, state }) => (
          <HStack gap={3} key={state}>
            <Box
              bg={STATE_COLOR_MAP[state] ?? "none.solid"}
              border="1px solid"
              borderColor="border.emphasized"
              borderRadius={SQUARE_BORDER_RADIUS}
              height={SQUARE_SIZE}
              width={SQUARE_SIZE}
            />
            <Text fontSize="xs">
              {count} {state}
            </Text>
          </HStack>
        ))}
      </VStack>
    </VStack>
  );
};
