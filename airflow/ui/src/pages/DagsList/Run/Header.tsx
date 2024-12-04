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
import { Box, Flex, Heading, HStack, SimpleGrid, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { FiBarChart } from "react-icons/fi";
import { MdOutlineModeComment } from "react-icons/md";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { Stat } from "src/components/Stat";
import Time from "src/components/Time";
import { Status } from "src/components/ui";

export const Header = ({ dagRun }: { readonly dagRun: DAGRunResponse }) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} overflow="hidden">
    <Box p={2}>
      <Flex alignItems="center" justifyContent="space-between" mb={2}>
        <HStack alignItems="center" gap={2}>
          <FiBarChart size="1.75rem" />
          <Heading size="lg">
            <strong>Run: </strong>
            {dagRun.dag_run_id}
          </Heading>
          <Status state={dagRun.state}>{dagRun.state}</Status>
          <Flex>
            <div />
          </Flex>
        </HStack>
      </Flex>
      {dagRun.note === null || dagRun.note.length === 0 ? undefined : (
        <Flex alignItems="flex-start" justifyContent="space-between" mr={16}>
          <MdOutlineModeComment size="3rem" />
          <Text fontSize="sm" ml={3}>
            {dagRun.note}
          </Text>
        </Flex>
      )}
      <SimpleGrid columns={4} gap={4}>
        <Stat label="Run Type">
          <HStack>
            <RunTypeIcon runType={dagRun.run_type} />
            <Text>{dagRun.run_type}</Text>
          </HStack>
        </Stat>
        <Stat label="Start">
          <Time datetime={dagRun.start_date} />
        </Stat>
        <Stat label="End">
          <Time datetime={dagRun.end_date} />
        </Stat>
        <Stat label="Duration">
          {dayjs
            .duration(dayjs(dagRun.end_date).diff(dagRun.start_date))
            .asSeconds()
            .toFixed(2)}
          s
        </Stat>
      </SimpleGrid>
    </Box>
  </Box>
);
