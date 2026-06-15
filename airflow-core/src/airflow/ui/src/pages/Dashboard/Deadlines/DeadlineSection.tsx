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
import { Box, Flex, Heading, HStack, Link, Separator, Skeleton, Text, VStack } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { DeadlineResponse } from "openapi/requests/types.gen";

import { DeadlineItem } from "./DeadlineItem";

type DeadlineSectionProps = {
  readonly deadlines: Array<DeadlineResponse>;
  readonly emptyLabel: string;
  readonly isLoading: boolean;
  readonly showMoreLabel: string;
  readonly showMoreTo: string;
  readonly title: string;
  readonly totalEntries: number;
};

export const DeadlineSection = ({
  deadlines,
  emptyLabel,
  isLoading,
  showMoreLabel,
  showMoreTo,
  title,
  totalEntries,
}: DeadlineSectionProps) => {
  const hasMoreDeadlines = totalEntries > deadlines.length;

  return (
    <Box flex={1} minWidth={0}>
      <Box borderRadius="md" borderWidth="1px" overflow="hidden">
        <HStack justify="space-between" px={3} py={2}>
          <Heading color="fg.muted" size="xs">
            {title}
          </Heading>
        </HStack>
        <Separator />
        {isLoading ? (
          <VStack align="stretch" data-testid="deadline-section-skeleton" gap={2} px={3} py={3}>
            <Skeleton height={4} width="85%" />
            <Skeleton height={4} width="65%" />
          </VStack>
        ) : deadlines.length === 0 ? (
          <Text color="fg.muted" fontSize="sm" px={3} py={3} textAlign="center">
            {emptyLabel}
          </Text>
        ) : (
          <VStack align="stretch" gap={0} separator={<Separator />}>
            {deadlines.map((deadline) => (
              <DeadlineItem deadline={deadline} key={deadline.id} />
            ))}
          </VStack>
        )}
        {hasMoreDeadlines ? (
          <>
            <Separator />
            <Flex justify="center" px={3} py={2}>
              <Link asChild color="fg.info" fontSize="xs" fontWeight="medium">
                <RouterLink to={showMoreTo}>{showMoreLabel}</RouterLink>
              </Link>
            </Flex>
          </>
        ) : undefined}
      </Box>
    </Box>
  );
};
