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
import { Box, Flex, HStack, Skeleton, Text } from "@chakra-ui/react";
import { FiChevronRight } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { capitalize } from "src/utils";

export const StatsCard = ({
  colorScheme,
  count,
  icon,
  isLoading = false,
  label,
  link,
  onClick,
  state,
}: {
  readonly colorScheme: string;
  readonly count: number;
  readonly icon?: React.ReactNode;
  readonly isLoading?: boolean;
  readonly label: string;
  readonly link?: string;
  readonly onClick?: () => void;
  readonly state?: TaskInstanceState | null;
}) => {
  const content = (
    <Box textAlign="left" width="100%">
      {isLoading ? (
        <Skeleton>
          <Flex
            alignItems="center"
            bg="bg.surface"
            borderColor={`${colorScheme}.border`}
            borderRadius="lg"
            borderWidth={1}
            height="60px"
            overflow="hidden"
            px={2}
            py={1}
          />
        </Skeleton>
      ) : (
        <Flex
          _hover={{
            boxShadow: "sm",
            transform: "translateY(-0.5px)",
            transition: "all 0.1s",
          }}
          alignItems="center"
          bg="bg.surface"
          borderColor={`${colorScheme}.border`}
          borderRadius="lg"
          borderWidth={1}
          cursor="pointer"
          height="60px"
          overflow="hidden"
          px={2}
          py={1}
          width="100%"
        >
          <StateBadge colorPalette={colorScheme} fontSize="md" mr={2} state={state} variant="solid">
            {icon}
            {count}
          </StateBadge>
          <Box flex={1}>
            <HStack alignItems="center" gap={2}>
              <Text
                fontSize="sm"
                fontWeight="bold"
                maxWidth="100%"
                overflow="hidden"
                textOverflow="ellipsis"
                whiteSpace="nowrap"
              >
                {capitalize(label)}
              </Text>
            </HStack>
          </Box>
          <Box color="gray.emphasized" px={1}>
            <FiChevronRight size={16} />
          </Box>
        </Flex>
      )}
    </Box>
  );

  if (onClick) {
    return <Box onClick={onClick}>{content}</Box>;
  }

  return <RouterLink to={link ?? "#"}>{content}</RouterLink>;
};
