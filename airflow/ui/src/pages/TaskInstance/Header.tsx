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
import { Box, Flex, Heading, HStack, SimpleGrid, Spinner } from "@chakra-ui/react";
import { FiMessageSquare } from "react-icons/fi";
import { MdOutlineTask } from "react-icons/md";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import { Stat } from "src/components/Stat";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({
  isRefreshing,
  taskInstance,
}: {
  readonly isRefreshing?: boolean;
  readonly taskInstance: TaskInstanceResponse;
}) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} p={2}>
    <Flex alignItems="center" justifyContent="space-between" mb={2}>
      <HStack alignItems="center" gap={2}>
        <MdOutlineTask size="1.75rem" />
        <Heading size="lg">
          <strong>Task Instance: </strong>
          {taskInstance.task_display_name} <Time datetime={taskInstance.start_date} />
        </Heading>
        <StateBadge state={taskInstance.state}>{taskInstance.state}</StateBadge>
        {isRefreshing ? <Spinner /> : <div />}
      </HStack>
      <HStack>
        {taskInstance.note === null || taskInstance.note.length === 0 ? undefined : (
          <DisplayMarkdownButton
            header="Task Instance Note"
            icon={<FiMessageSquare color="black" />}
            mdContent={taskInstance.note}
            text="Note"
          />
        )}
        <ClearTaskInstanceButton taskInstance={taskInstance} />
        <MarkTaskInstanceAsButton taskInstance={taskInstance} />
      </HStack>
    </Flex>
    <SimpleGrid columns={6} gap={4} my={2}>
      <Stat label="Operator">{taskInstance.operator}</Stat>
      {taskInstance.map_index > -1 ? (
        <Stat label="Map Index">{taskInstance.rendered_map_index}</Stat>
      ) : undefined}
      {taskInstance.try_number > 1 ? <Stat label="Try Number">{taskInstance.try_number}</Stat> : undefined}
      <Stat label="Start">
        <Time datetime={taskInstance.start_date} />
      </Stat>
      <Stat label="End">
        <Time datetime={taskInstance.end_date} />
      </Stat>
      <Stat label="Duration">{getDuration(taskInstance.start_date, taskInstance.end_date)}s</Stat>
      {taskInstance.dag_version?.version_number !== undefined && (
        <Stat label="Dag Version">{`v${taskInstance.dag_version.version_number}`}</Stat>
      )}
    </SimpleGrid>
  </Box>
);
