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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { StateBadge } from "src/components/StateBadge";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { useOpenGroups } from "src/context/openGroups";

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
  const { t: translate } = useTranslation("components");
  const { toggleGroupId } = useOpenGroups();
  const onClick = () => {
    if (isGroup) {
      toggleGroupId(id);
    }
  };
  const thisChildCount = useMemo(
    () =>
      Object.entries(taskInstance?.child_states ?? {})
        .map(([_state, count]) => count)
        .reduce((sum, val) => sum + val, 0),
    [taskInstance],
  );

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
                childCount={thisChildCount}
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
              whiteSpace="nowrap"
            >
              {isGroup ? translate("graph.taskGroup") : operator}
            </Text>
            {taskInstance === undefined ? undefined : (
              <HStack>
                <StateBadge fontSize="xs" state={taskInstance.state}>
                  {taskInstance.state}
                </StateBadge>
              </HStack>
            )}
            {isGroup ? (
              <Button
                colorPalette="brand"
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
                {translate("graph.taskCount", { count: childCount ?? 0 })}
              </Button>
            ) : undefined}
          </Box>
        </TaskInstanceTooltip>
        {Boolean(isMapped) || Boolean(isGroup && !isOpen) ? (
          <>
            <Box
              bg={taskInstance?.state ? `${taskInstance.state}.solid` : "bg.subtle"}
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor={taskInstance?.state ? `${taskInstance.state}.solid` : "border.emphasized"}
              borderLeftWidth={1}
              borderRightWidth={1}
              height={1}
              width={`${width - 10}px`}
            />
            <Box
              bg={taskInstance?.state ? `${taskInstance.state}.solid` : "bg.subtle"}
              borderBottomLeftRadius={5}
              borderBottomRightRadius={5}
              borderBottomWidth={1}
              borderColor={taskInstance?.state ? `${taskInstance.state}.solid` : "border.emphasized"}
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
