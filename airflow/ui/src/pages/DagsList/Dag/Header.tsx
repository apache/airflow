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
import {
  Box,
  Button,
  Flex,
  Heading,
  HStack,
  SimpleGrid,
  Text,
  Tooltip,
  useColorModeValue,
  VStack,
} from "@chakra-ui/react";
import { FiCalendar, FiPlay } from "react-icons/fi";

import type { DAGResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";

import { DagTags } from "../DagTags";

export const Header = ({
  dag,
  dagId,
}: {
  readonly dag?: DAGResponse;
  readonly dagId?: string;
}) => {
  const grayBg = useColorModeValue("gray.100", "gray.900");
  const grayBorder = useColorModeValue("gray.200", "gray.700");

  return (
    <Box
      borderColor={grayBorder}
      borderRadius={8}
      borderWidth={1}
      overflow="hidden"
    >
      <Box p={2}>
        <Flex alignItems="center" justifyContent="space-between">
          <HStack alignItems="center" spacing={2}>
            <DagIcon height={8} width={8} />
            <Heading size="lg">{dag?.dag_display_name ?? dagId}</Heading>
            {dag !== undefined && (
              <TogglePause dagId={dag.dag_id} isPaused={dag.is_paused} />
            )}
          </HStack>
          <Flex>
            <Button colorScheme="blue" isDisabled leftIcon={<FiPlay />}>
              Trigger
            </Button>
          </Flex>
        </Flex>
        <SimpleGrid columns={4} height={8} my={4} spacing={4}>
          <VStack align="flex-start" spacing={1}>
            <Heading color="gray.500" fontSize="xs">
              Last Run
            </Heading>
          </VStack>
          <VStack align="flex-start" spacing={1}>
            <Heading color="gray.500" fontSize="xs">
              Next Run
            </Heading>
            {Boolean(dag?.next_dagrun) ? (
              <Text fontSize="sm">
                <Time datetime={dag?.next_dagrun} />
              </Text>
            ) : undefined}
          </VStack>
          <VStack align="flex-start" spacing={1}>
            <Heading color="gray.500" fontSize="xs">
              Schedule
            </Heading>
            {Boolean(dag?.timetable_summary) ? (
              <Tooltip hasArrow label={dag?.timetable_description}>
                <Text fontSize="sm">
                  <FiCalendar style={{ display: "inline" }} />{" "}
                  {dag?.timetable_summary}
                </Text>
              </Tooltip>
            ) : undefined}
          </VStack>
          <div />
        </SimpleGrid>
      </Box>
      <Flex
        alignItems="center"
        bg={grayBg}
        borderTopColor={grayBorder}
        borderTopWidth={1}
        color="gray.400"
        fontSize="sm"
        justifyContent="space-between"
        px={2}
        py={1}
      >
        <Text>Owner: {dag?.owners.join(", ")}</Text>
        <DagTags tags={dag?.tags ?? []} />
      </Flex>
    </Box>
  );
};
