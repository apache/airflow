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
  Badge,
  Box,
  Flex,
  HStack,
  Heading,
  SimpleGrid,
  Text,
  Tooltip,
  useColorModeValue,
  VStack,
} from "@chakra-ui/react";
import { FiCalendar, FiTag } from "react-icons/fi";

import type { DAGResponse } from "openapi/requests/types.gen";
import { TogglePause } from "src/components/TogglePause";

type Props = {
  readonly dag: DAGResponse;
};

const MAX_TAGS = 3;

export const DagCard = ({ dag }: Props) => {
  const cardBorder = useColorModeValue("gray.100", "gray.700");
  const tooltipBg = useColorModeValue("gray.200", "gray.700");

  return (
    <Box
      borderColor={cardBorder}
      borderRadius={8}
      borderWidth={1}
      overflow="hidden"
    >
      <Flex
        alignItems="center"
        bg="subtle-bg"
        justifyContent="space-between"
        px={3}
        py={2}
      >
        <HStack>
          <Tooltip hasArrow label={dag.description}>
            <Heading color="subtle-text" fontSize="md">
              {dag.dag_display_name}
            </Heading>
          </Tooltip>
          {dag.tags.length ? (
            <HStack spacing={1}>
              <FiTag data-testid="dag-tag" />
              {dag.tags.slice(0, MAX_TAGS).map((tag) => (
                <Badge key={tag.name}>{tag.name}</Badge>
              ))}
              {dag.tags.length > MAX_TAGS && (
                <Tooltip
                  bg={tooltipBg}
                  hasArrow
                  label={
                    <VStack p={1} spacing={1}>
                      {dag.tags.slice(MAX_TAGS).map((tag) => (
                        <Badge key={tag.name}>{tag.name}</Badge>
                      ))}
                    </VStack>
                  }
                >
                  <Badge>+{dag.tags.length - MAX_TAGS} more</Badge>
                </Tooltip>
              )}
            </HStack>
          ) : undefined}
        </HStack>
        <HStack>
          <TogglePause dagId={dag.dag_id} isPaused={dag.is_paused} />
        </HStack>
      </Flex>
      <SimpleGrid columns={4} height={20} px={3} py={2} spacing={4}>
        <div />
        <VStack align="flex-start" spacing={1}>
          <Heading color="gray.500" fontSize="xs">
            Next Run
          </Heading>
          {Boolean(dag.next_dagrun) ? (
            <Text fontSize="sm">{dag.next_dagrun}</Text>
          ) : undefined}
          {Boolean(dag.timetable_summary) ? (
            <Tooltip hasArrow label={dag.timetable_description}>
              <Text fontSize="sm">
                {" "}
                <FiCalendar style={{ display: "inline" }} />{" "}
                {dag.timetable_summary}
              </Text>
            </Tooltip>
          ) : undefined}
        </VStack>
        <div />
        <div />
      </SimpleGrid>
    </Box>
  );
};
