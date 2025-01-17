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
import { FiBookOpen } from "react-icons/fi";

import type { TaskResponse } from "openapi/requests/types.gen";
import { TaskIcon } from "src/assets/TaskIcon";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { Stat } from "src/components/Stat";

export const Header = ({ task }: { readonly task: TaskResponse }) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} p={2}>
    <Flex alignItems="center" justifyContent="space-between" mb={2}>
      <HStack alignItems="center" gap={2}>
        <TaskIcon height={8} width={8} />
        <Heading size="lg">
          <strong>Task: </strong>
          {task.task_display_name}
          {task.is_mapped ? "[ ]" : ""}
        </Heading>
        <Flex>
          <div />
        </Flex>
      </HStack>
      {task.doc_md === null ? undefined : (
        <DisplayMarkdownButton
          header="Task Documentation"
          icon={<FiBookOpen />}
          mdContent={task.doc_md}
          text="Task Docs"
        />
      )}
    </Flex>
    <SimpleGrid columns={4} gap={4}>
      <Stat label="Operator">
        <Text>{task.operator_name}</Text>
      </Stat>
      <Stat label="Trigger Rule">
        <Text>{task.trigger_rule}</Text>
      </Stat>
    </SimpleGrid>
  </Box>
);
