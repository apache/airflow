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
import { Box, Flex, HStack, VStack, Text } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { TaskInstanceStateCount } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { capitalize } from "src/utils";

const BAR_WIDTH = 100;
const BAR_HEIGHT = 5;

type MetricSectionProps = {
  readonly endDate?: string;
  readonly kind: string;
  readonly runs: number;
  readonly startDate: string;
  readonly state: keyof TaskInstanceStateCount;
  readonly total: number;
};

export const MetricSection = ({ endDate, kind, runs, startDate, state, total }: MetricSectionProps) => {
  // Calculate the given state as a percentage of total and draw a bar
  // in state's color with width as state's percentage and remaining width filed as gray
  const statePercent = total === 0 ? 0 : ((runs / total) * 100).toFixed(2);
  const stateWidth = total === 0 ? 0 : (runs / total) * BAR_WIDTH;
  const remainingWidth = BAR_WIDTH - stateWidth;

  const searchParams = new URLSearchParams(`?state=${state}&start_date=${startDate}`);

  if (endDate !== undefined) {
    searchParams.append("end_date", endDate);
  }

  return (
    <VStack align="left" gap={1} ml={0} pl={0}>
      <Flex justify="space-between">
        <HStack>
          <RouterLink to={`/${kind}?${searchParams.toString()}`}>
            {/* eslint-disable-next-line unicorn/no-null */}
            <StateBadge fontSize="md" state={state === "no_status" ? null : state}>
              {runs}
            </StateBadge>
          </RouterLink>
          <Text>
            {state
              .split("_")
              .map((st) => capitalize(st))
              .join(" ")}
          </Text>
        </HStack>
        <Text color="fg.muted"> {statePercent}% </Text>
      </Flex>
      <HStack gap={0} mt={2}>
        <Box
          bg={`${state === "no_status" ? "none" : state}.solid`}
          borderLeftRadius={5}
          height={`${BAR_HEIGHT}px`}
          minHeight={2}
          width={`${stateWidth}%`}
        />
        <Box
          bg="bg.emphasized"
          borderLeftRadius={runs === 0 ? 5 : 0} // When there are no states then have left radius too since this is the only bar displayed
          borderRightRadius={5}
          height={`${BAR_HEIGHT}px`}
          minHeight={2}
          width={`${remainingWidth}%`}
        />
      </HStack>
    </VStack>
  );
};
