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
import { Box, Flex, Heading, HStack, Spinner, Stack } from "@chakra-ui/react";
import { useRef } from "react";
import type { ReactNode } from "react";
import { useLocation } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { useContainerWidth } from "src/utils/useContainerWidth";

import StatsDisplay from "./StatsDisplay";

type Props = {
  readonly actions?: ReactNode;
  readonly icon: ReactNode;
  readonly isRefreshing?: boolean;
  readonly state?: TaskInstanceState | null;
  readonly stats: Array<{ label: string; value: ReactNode | string }>;
  readonly subTitle?: ReactNode | string;
  readonly title: ReactNode | string;
};

export const HeaderCard = ({ actions, icon, isRefreshing, state, stats, subTitle, title }: Props) => {
  const containerRef = useRef<HTMLDivElement>();
  const containerWidth = useContainerWidth(containerRef);
  const isGanttPage = useLocation().pathname.includes("/gantt");

  return (
    <Box
      borderColor="border"
      borderRadius={8}
      borderWidth={1}
      h={isGanttPage ? "150px" : undefined}
      ml={2}
      p={2}
      ref={containerRef}
    >
      <Flex alignItems="center" flexWrap="wrap" justifyContent="space-between" mb={2}>
        <Flex alignItems="center" flexWrap="wrap" gap={2}>
          <Heading size="xl">{icon}</Heading>
          <Heading size="lg">{title}</Heading>
          <Heading size="lg">{subTitle}</Heading>
          {state === undefined ? undefined : <StateBadge state={state}>{state}</StateBadge>}
          {isRefreshing ? <Spinner /> : <div />}
        </Flex>
        <Stack direction={isGanttPage && containerWidth <= 600 ? "row" : "column"} mt={2} w="100%">
          <HStack gap={1}>{actions}</HStack>
          <StatsDisplay
            isCompact={isGanttPage ? containerWidth < 600 : false}
            stats={stats}
            title={typeof title === "string" ? title : undefined}
          />
        </Stack>
      </Flex>
    </Box>
  );
};
