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
import { Box, Button, Flex, HStack, LinkOverlay, Text, useToken } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { useTranslation } from "react-i18next";
import { AiOutlineGroup } from "react-icons/ai";

import { TaskIcon } from "src/assets/TaskIcon";
import { StateBadge } from "src/components/StateBadge";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { useGroups } from "src/context/groups";

import { NodeWrapper } from "./NodeWrapper";
import { SegmentedStateBar } from "./SegmentedStateBar";
import { TaskLink } from "./TaskLink";
import { opacityStyle } from "./graphTypes";
import { readableTextForFill } from "./nodeColors";
import type { CustomNodeProps } from "./reactflowUtils";

// A colored group is softened toward the surface and alternates between these two shares of the custom
// color by nesting depth, so open nested groups stay visually distinct (like the bg.muted/bg stripes).
const GROUP_FILL_STRONG = 45;
const GROUP_FILL_SOFT = 22;

export const TaskNode = ({
  data: {
    childCount,
    depth,
    height = 0,
    isFiltered,
    isGroup,
    isMapped,
    isOpen,
    isSelected,
    label,
    operator,
    setupTeardownType,
    taskInstance,
    team,
    tooltip,
    uiColor,
    uiFgcolor,
    width = 0,
  },
  id,
}: NodeProps<NodeType<CustomNodeProps, "task">>) => {
  const { t: translate } = useTranslation("components");
  const { toggleGroupId } = useGroups();
  // Resolve the fill through Chakra so any token (dotted like "blue.500" or bare like "bg") becomes a
  // valid CSS color for the group color-mix; raw hex/CSS names pass through unchanged.
  const [resolvedFill] = useToken("colors", [uiColor ?? "transparent"]);
  const onClick = () => {
    if (isGroup) {
      toggleGroupId(id);
    }
  };

  // For task dependency nodes, parse dag_id from label (format: dag_id.task_id)
  const parseDagIdFromLabel = (nodeLabel: string): { dagId: string | undefined; taskId: string } => {
    if (operator !== undefined) {
      return { dagId: undefined, taskId: nodeLabel };
    }
    const dotIndex = nodeLabel.indexOf(".");

    if (dotIndex > 0) {
      return {
        dagId: nodeLabel.slice(0, Math.max(0, dotIndex)),
        taskId: nodeLabel.slice(Math.max(0, dotIndex + 1)),
      };
    }

    return { dagId: undefined, taskId: nodeLabel };
  };

  const { dagId, taskId } = parseDagIdFromLabel(label);
  const displayLabel = dagId === undefined ? label : taskId;
  const displayOperator = operator ?? dagId;

  const thisChildCount = Object.entries(taskInstance?.child_states ?? {})
    .map(([_state, count]) => count)
    .reduce((sum, val) => sum + val, 0);

  // Custom colors can mess up the readability of the text, so we calculate a readable foreground color for the node based on the background color.
  // Pass the resolved color so Chakra tokens are measured by their hex rather than skipped.
  const readableFgColor = isGroup ? undefined : readableTextForFill(resolvedFill);
  // Alternate nested groups colors so nested groups are visually distinct and readable
  const shouldAlternate = isOpen && depth !== undefined && depth % 2 === 0;

  let nodeBg: string;

  if (!isGroup) {
    nodeBg = uiColor ?? "bg";
  } else if (uiColor === undefined) {
    nodeBg = shouldAlternate ? "bg.muted" : "bg";
  } else {
    const share = shouldAlternate ? GROUP_FILL_STRONG : GROUP_FILL_SOFT;

    nodeBg = `color-mix(in srgb, ${resolvedFill} ${share}%, var(--chakra-colors-bg))`;
  }

  return (
    <NodeWrapper>
      <Flex alignItems="center" cursor="default" flexDirection="column" {...opacityStyle(isFiltered)}>
        <TaskInstanceTooltip
          openDelay={500}
          positioning={{
            placement: "top-start",
          }}
          taskInstance={taskInstance}
          tooltip={isGroup ? tooltip : undefined}
        >
          <Flex
            bg={nodeBg}
            borderColor={
              isSelected ? "blue.500" : taskInstance?.state ? `${taskInstance.state}.solid` : "border"
            }
            borderRadius={5}
            borderWidth={isSelected ? 4 : 2}
            color={readableFgColor}
            direction="column"
            height={`${height + (isSelected ? 4 : 0)}px`}
            overflow="hidden"
            position="relative"
            px={isSelected ? 1 : 2}
            py={isSelected ? 0 : 1}
            width={`${width + (isSelected ? 4 : 0)}px`}
          >
            <HStack>
              {isGroup ? <AiOutlineGroup /> : <TaskIcon />}
              <LinkOverlay asChild>
                <TaskLink
                  childCount={thisChildCount}
                  id={id}
                  isGroup={isGroup}
                  isMapped={isMapped}
                  isOpen={isOpen}
                  label={displayLabel}
                  setupTeardownType={setupTeardownType}
                />
              </LinkOverlay>
            </HStack>
            <Text
              color={uiFgcolor ?? readableFgColor ?? "fg.muted"}
              fontSize="sm"
              overflow="hidden"
              textOverflow="ellipsis"
              whiteSpace="nowrap"
            >
              {isGroup ? translate("graph.taskGroup") : displayOperator}
            </Text>
            {team !== undefined && team !== null ? (
              <Text
                color="fg.muted"
                fontSize="xs"
                fontStyle="italic"
                overflow="hidden"
                textOverflow="ellipsis"
                whiteSpace="nowrap"
              >
                {team}
              </Text>
            ) : undefined}
            {taskInstance === undefined ? undefined : (
              <HStack>
                <StateBadge fontSize="xs" state={taskInstance.state}>
                  {taskInstance.state}
                </StateBadge>
              </HStack>
            )}
            {isGroup ? (
              <Button
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
            {Boolean(isMapped) || Boolean(isGroup && !isOpen) ? (
              <SegmentedStateBar
                childStates={taskInstance?.child_states ?? null}
                fallbackState={taskInstance?.state}
              />
            ) : undefined}
          </Flex>
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
