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
import { Box, Flex, HStack, SimpleGrid, Text, VStack } from "@chakra-ui/react";
import { FiCalendar, FiTag } from "react-icons/fi";

import type { DAGResponse } from "openapi/requests/types.gen";
import { TogglePause } from "src/components/TogglePause";

type Props = {
  readonly dag: DAGResponse;
};

export const DagCard = ({ dag }: Props) => (
  <Box
    borderColor="gray.100"
    borderRadius={8}
    borderWidth={1}
    overflow="hidden"
  >
    <Flex
      alignItems="center"
      bg="blue.50"
      justifyContent="space-between"
      px={3}
      py={2}
    >
      <HStack>
        <Text color="blue.600" fontWeight="bold">
          {dag.dag_display_name}
        </Text>
        {dag.tags.length ? (
          <HStack spacing={1}>
            <FiTag />
            {dag.tags.map((tag, index) => (
              <Text fontSize="sm" key={tag.name}>
                {tag.name}
                {index === dag.tags.length - 1 ? undefined : ","}
              </Text>
            ))}
          </HStack>
        ) : undefined}
      </HStack>
      <HStack>
        <TogglePause dagId={dag.dag_id} isPaused={dag.is_paused} />
      </HStack>
    </Flex>
    <SimpleGrid columns={4} height={20} px={3} py={2} spacing={4}>
      <Box />
      {Boolean(dag.next_dagrun) ? (
        <VStack align="flex-start" spacing={1}>
          <Text color="gray.500" fontSize="sm">
            Next Run
          </Text>
          <Text fontSize="sm">
            {dag.next_dagrun} <FiCalendar style={{ display: "inline" }} />{" "}
            {dag.timetable_description}
          </Text>
        </VStack>
      ) : undefined}
      <Box />
      <Box />
    </SimpleGrid>
  </Box>
);
