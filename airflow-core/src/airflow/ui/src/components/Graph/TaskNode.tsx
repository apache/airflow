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
import { Box, Button, Flex, HStack, LinkOverlay, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { CgRedo } from "react-icons/cg";

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
    height = 0,
    isGroup,
    isMapped,
    isOpen,
    isSelected,
    label,
    operator,
    setupTeardownType,
    taskInstance,
    width = 0,
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
          <Box
            // Alternate background color for nested open groups
            bg={isOpen && depth !== undefined && depth % 2 === 0 ? "bg.muted" : "bg"}
            borderColor={
              taskInstance?.state ? `${taskInstance.state}.solid` : isSelected ? "border.inverted" : "border"
            }
            borderRadius={5}
            borderWidth={isSelected ? 4 : 2}
            height={`${height + (isSelected ? 4 : 0)}px`}
            justifyContent="space-between"
            overflow="hidden"
            position="relative"
            px={isSelected ? 1 : 2}
            py={isSelected ? 0 : 1}
            width={`${width + (isSelected ? 4 : 0)}px`}
          >
            <LinkOverlay asChild>
              <TaskLink
                childCount={taskInstance?.task_count}
                id={id}
                isGroup={isGroup}
                isMapped={isMapped}
                isOpen={isOpen}
                label={label}
                setupTeardownType={setupTeardownType}
              />
            </LinkOverlay>
            <Text
              color="fg.muted"
              fontSize="sm"
              overflow="hidden"
              textOverflow="ellipsis"
              textTransform="capitalize"
              whiteSpace="nowrap"
            >
              {isGroup ? "Task Group" : operator}
            </Text>
            {taskInstance === undefined ? undefined : (
              <HStack>
                <StateBadge fontSize="xs" state={taskInstance.state}>
                  {taskInstance.state}
                </StateBadge>
                {taskInstance.try_number > 1 ? <CgRedo /> : undefined}
              </HStack>
            )}
            {isGroup ? (
              <Button
                colorPalette="blue"
                cursor="pointer"
                height={8}
                onClick={onClick}
                position="absolute"
                px={1}
                right={0}
                top={0}
                variant="plain"
              >
                {isOpen ? "- " : "+ "}
                {pluralize("task", childCount, undefined, false)}
              </Button>
            ) : undefined}
          </Box>
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
              width={`${width - 10}px`}
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
              width={`${width - 20}px`}
            />
          </>
        ) : undefined}
      </Flex>
    </NodeWrapper>
  );
};
