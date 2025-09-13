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

import { createRichTooltipContent } from "./richTooltipUtils";
import type { CalendarCellData } from "./types";

const SQUARE_SIZE = "12px";
const SQUARE_BORDER_RADIUS = "2px";

type Props = {
  readonly cellData: CalendarCellData;
};

export const CalendarTooltipContent = ({ cellData }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { date, hasRuns, states, total } = createRichTooltipContent(cellData);

  if (!hasRuns) {
    return (
      <Text fontSize="sm">
        {date}: {translate("calendar.noRuns")}
      </Text>
    );
  }

  return (
    <VStack align="start" gap={2}>
      <Text fontSize="sm" fontWeight="medium">
        {date}: {total} {translate("calendar.runs")}
      </Text>
      <VStack align="start" gap={1.5}>
        {states.map(({ color, count, state }) => (
          <HStack gap={3} key={state}>
            <Box
              bg={color}
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
