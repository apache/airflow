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
import { Box, Flex, GridItem, Heading, HStack, SimpleGrid, Spinner } from "@chakra-ui/react";
import { type ReactNode, useRef } from "react";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { Stat } from "src/components/Stat";
import { StateBadge } from "src/components/StateBadge";
import { useContainerWidth } from "src/utils";

const getColumnCount = (width: number) => {
  if (width < 400) {
    return 2;
  }
  if (width < 800) {
    return 4;
  }
  if (width < 1000) {
    return 6;
  }

  return 8;
};

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

  return (
    <Box borderColor="border" borderRadius={8} borderWidth={1} m={2} p={2} ref={containerRef}>
      <Flex alignItems="center" flexWrap="wrap" justifyContent="space-between" mb={2}>
        <Flex alignItems="center" flexWrap="wrap" gap={2}>
          <Heading size="xl">{icon}</Heading>
          <Heading size="lg">{title}</Heading>
          <Heading size="lg">{subTitle}</Heading>
          {state === undefined ? undefined : <StateBadge state={state}>{state}</StateBadge>}
          {isRefreshing ? <Spinner /> : <div />}
        </Flex>
        <HStack gap={1}>{actions}</HStack>
      </Flex>
      <SimpleGrid
        autoFlow="row dense"
        gap={4}
        my={2}
        templateColumns={`repeat(${getColumnCount(containerWidth)}, 1fr)`}
      >
        {stats.map(({ label, value }) => (
          <GridItem key={label}>
            <Stat label={label}>{value}</Stat>
          </GridItem>
        ))}
      </SimpleGrid>
    </Box>
  );
};
