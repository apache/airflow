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
import { MdOutlineModeComment, MdOutlineTask } from "react-icons/md";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { Stat } from "src/components/Stat";
import Time from "src/components/Time";
import { Status } from "src/components/ui";

export const Header = ({
  taskInstance,
}: {
  readonly taskInstance: TaskInstanceResponse;
}) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} p={2}>
    <Flex alignItems="center" justifyContent="space-between" mb={2}>
      <HStack alignItems="center" gap={2}>
        <MdOutlineTask size="1.75rem" />
        <Heading size="lg">
          <strong>Task Instance: </strong>
          {taskInstance.task_display_name}{" "}
          <Time datetime={taskInstance.start_date} />
        </Heading>
        <Status state={taskInstance.state}>{taskInstance.state}</Status>
        <Flex>
          <div />
        </Flex>
      </HStack>
    </Flex>
    {taskInstance.note === null ||
    taskInstance.note.length === 0 ? undefined : (
      <Flex alignItems="flex-start" justifyContent="space-between" mr={16}>
        <MdOutlineModeComment size="3rem" />
        <Text fontSize="sm" ml={3}>
          {taskInstance.note}
        </Text>
      </Flex>
    )}
    <SimpleGrid columns={6} gap={4} my={2}>
      <Stat label="Operator">{taskInstance.operator}</Stat>
      {taskInstance.map_index > -1 ? (
        <Stat label="Map Index">{taskInstance.map_index}</Stat>
      ) : undefined}
      {taskInstance.try_number > 1 ? (
        <Stat label="Try Number">{taskInstance.try_number}</Stat>
      ) : undefined}
      <Stat label="Start">
        <Time datetime={taskInstance.start_date} />
      </Stat>
      <Stat label="End">
        <Time datetime={taskInstance.end_date} />
      </Stat>
      <Stat label="Duration">
        {dayjs
          .duration(dayjs(taskInstance.end_date).diff(taskInstance.start_date))
          .asSeconds()
          .toFixed(2)}
        s
      </Stat>
    </SimpleGrid>
  </Box>
);
