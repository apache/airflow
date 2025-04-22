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

import { capitalize } from "src/utils";

export const StatsCard = ({
  colorScheme,
  count,
  isLoading = false,
  label,
  link,
  onClick,
}: {
  readonly colorScheme: string;
  readonly count: number;
  readonly isLoading?: boolean;
  readonly label: string;
  readonly link?: string;
  readonly onClick?: () => void;
}) => {
  const content = (
    <Box as="button" textAlign="left" width="100%">
      {isLoading ? (
        <Skeleton>
          <Flex
            alignItems="center"
            bg="bg.surface"
            borderColor={`${colorScheme}.100`}
            borderRadius="lg"
            borderWidth={1}
            height="60px"
            overflow="hidden"
            p={3}
            pl={4}
          />
        </Skeleton>
      ) : (
        <Flex
          _hover={{
            borderColor: `${colorScheme}.500`,
            boxShadow: "sm",
            transform: "translateY(-0.5px)",
            transition: "all 0.1s",
          }}
          alignItems="center"
          bg="bg.surface"
          borderColor={`${colorScheme}.100`}
          borderRadius="lg"
          borderWidth={1}
          cursor="pointer"
          height="60px"
          overflow="hidden"
          p={3}
          pl={4}
          width="100%"
        >
          <Flex
            alignItems="center"
            bg={`${colorScheme}.500`}
            borderRadius="full"
            boxSize={8}
            color="white"
            fontWeight="bold"
            justifyContent="center"
            minWidth={8}
            mr={3}
          >
            <Text fontSize="sm" fontWeight="bold">
              {count}
            </Text>
          </Flex>
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
          <Box color="gray.400" pr={1}>
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
