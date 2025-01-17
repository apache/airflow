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
import { Box, Button, Flex, HStack, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { MdRefresh } from "react-icons/md";

import { StateBadge } from "src/components/StateBadge";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { useOpenGroups } from "src/context/openGroups";
import { pluralize } from "src/utils";

import { NodeWrapper } from "./NodeWrapper";
import { TaskLink } from "./TaskLink";
import type { CustomNodeProps } from "./reactflowUtils";

export const TaskNode = ({
  data: {
    childCount,
    depth,
    height,
    isGroup,
    isMapped,
    isOpen,
    isSelected,
    label,
    operator,
    setupTeardownType,
    taskInstance,
    width,
  },
  id,
}: NodeProps<NodeType<CustomNodeProps, "task">>) => {
  const { toggleGroupId } = useOpenGroups();
  const onClick = () => {
    if (isGroup) {
      toggleGroupId(id);
    }
  };

  return (
    <NodeWrapper>
      <Flex alignItems="center" cursor="default" flexDirection="column">
        <TaskInstanceTooltip
          openDelay={500}
          positioning={{
            placement: "top-start",
          }}
          taskInstance={taskInstance}
        >
          <Flex
            // Alternate background color for nested open groups
            bg={isOpen && depth !== undefined && depth % 2 === 0 ? "bg.muted" : "bg"}
            borderColor={taskInstance?.state ? `${taskInstance.state}.solid` : "border"}
            borderRadius={5}
            borderWidth={isSelected ? 6 : 2}
            height={`${height}px`}
            justifyContent="space-between"
            px={3}
            py={isSelected ? 1 : 2}
            width={`${width}px`}
          >
            <Box>
              <TaskLink
                id={id}
                isGroup={isGroup}
                isMapped={isMapped}
                isOpen={isOpen}
                label={label}
                setupTeardownType={setupTeardownType}
              />
              <Text color="fg.muted" fontSize="sm" textTransform="capitalize">
                {isGroup ? "Task Group" : operator}
              </Text>
              {taskInstance === undefined ? undefined : (
                <HStack>
                  <StateBadge fontSize="xs" state={taskInstance.state}>
                    {taskInstance.state}
                  </StateBadge>
                  {taskInstance.try_number > 1 ? <MdRefresh /> : undefined}
                </HStack>
              )}
            </Box>
            <Box>
              {isGroup ? (
                <Button
                  colorPalette="blue"
                  cursor="pointer"
                  height="inherit"
                  onClick={onClick}
                  pb={2}
                  pr={0}
                  variant="plain"
                >
                  {isOpen ? "- " : "+ "}
                  {pluralize("task", childCount, undefined, false)}
                </Button>
              ) : undefined}
            </Box>
          </Flex>
        </TaskInstanceTooltip>
        {Boolean(isMapped) || Boolean(isGroup && !isOpen) ? (
          <>
            <Box
              bg="bg.subtle"
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor="fg"
              borderLeftWidth={1}
              borderRightWidth={1}
              height={1}
              width={`${(width ?? 0) - 10}px`}
            />
            <Box
              bg="bg.subtle"
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor="fg"
              borderLeftWidth={1}
              borderRightWidth={1}
              height={1}
              width={`${(width ?? 0) - 20}px`}
            />
          </>
        ) : undefined}
      </Flex>
    </NodeWrapper>
  );
};
