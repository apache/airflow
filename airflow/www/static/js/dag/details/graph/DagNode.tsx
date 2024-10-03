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

import React from "react";
import { Flex, Text, useTheme } from "@chakra-ui/react";
import type { NodeProps } from "reactflow";

import { SimpleStatus } from "src/dag/StatusBox";
import useSelection from "src/dag/useSelection";
import { getGroupAndMapSummary, hoverDelay } from "src/utils";
import Tooltip from "src/components/Tooltip";
import InstanceTooltip from "src/components/InstanceTooltip";
import { useContainerRef } from "src/context/containerRef";
import TaskName from "src/dag/TaskName";

import type { CustomNodeProps } from "./Node";

const DagNode = ({
  id,
  data: {
    label,
    childCount,
    height,
    width,
    instance,
    task,
    isSelected,
    latestDagRunId,
    isOpen,
    isActive,
    setupTeardownType,
    onToggleCollapse,
    labelStyle,
    style,
    isZoomedOut,
  },
}: NodeProps<CustomNodeProps>) => {
  const { onSelect } = useSelection();
  const containerRef = useContainerRef();
  const { colors } = useTheme();

  if (!task) return null;

  const groupBg = isOpen ? `${colors.blue[500]}15` : "blue.50";
  const { isMapped } = task;
  const mappedStates = instance?.mappedStates;

  const { totalTasks } = getGroupAndMapSummary({ group: task, mappedStates });

  const taskName = isMapped
    ? `${label} [${instance ? totalTasks : " "}]`
    : label;

  let operatorTextColor = "";
  let operatorBg = "";
  if (style) {
    [, operatorBg] = style.split(":");
  }

  if (labelStyle) {
    [, operatorTextColor] = labelStyle.split(":");
  }
  if (!operatorTextColor || operatorTextColor === "#000;")
    operatorTextColor = "gray.500";

  const nodeBorderColor =
    instance?.state && stateColors[instance.state]
      ? stateColors[instance.state]
      : "gray.400";

  let borderWidth = 2;
  if (isZoomedOut) {
    if (isSelected) borderWidth = 10;
    else borderWidth = 6;
  } else if (isSelected) borderWidth = 4;

  return (
    <Tooltip
      label={
        instance && task ? (
          <InstanceTooltip instance={instance} group={task} />
        ) : null
      }
      portalProps={{ containerRef }}
      hasArrow
      placement="top"
      openDelay={hoverDelay}
    >
      <Flex
        borderRadius={isZoomedOut ? 10 : 5}
        borderWidth={borderWidth}
        borderColor={nodeBorderColor}
        wordBreak="break-word"
        bg={
          !task.children?.length && operatorBg
            ? // Fade the operator color to clash less with the task instance status
              `color-mix(in srgb, ${operatorBg.replace(";", "")} 80%, white)`
            : groupBg
        }
        height={`${height}px`}
        width={`${width}px`}
        cursor={latestDagRunId ? "cursor" : "default"}
        opacity={isActive ? 1 : 0.3}
        transition="opacity 0.2s"
        data-testid="node"
        onClick={() => {
          if (latestDagRunId) {
            onSelect({
              runId: instance?.runId || latestDagRunId,
              taskId: isSelected ? undefined : id,
            });
          }
        }}
        px={isZoomedOut ? 1 : 2}
        mt={isZoomedOut ? -2 : 0}
        alignItems={isZoomedOut && !isOpen ? "center" : undefined}
        justifyContent={isZoomedOut && !isOpen ? "center" : undefined}
        flexDirection="column"
        overflow="wrap"
      >
        <TaskName
          label={taskName}
          isOpen={isOpen}
          isGroup={!!childCount}
          onClick={(e) => {
            if (childCount) {
              e.stopPropagation();
              onToggleCollapse();
            }
          }}
          setupTeardownType={setupTeardownType}
          isZoomedOut={isZoomedOut}
          noOfLines={2}
        />
        {!isZoomedOut && (
          <>
            {!!instance && instance.state && (
              <Flex alignItems="center">
                <SimpleStatus state={instance.state} />
                <Text ml={2} color="gray.500" fontWeight={400} fontSize="md">
                  {instance.state}
                </Text>
              </Flex>
            )}
            {task?.operator && (
              <Text
                noOfLines={1}
                maxWidth={`calc(${width}px - 12px)`}
                fontWeight={400}
                fontSize="md"
                color={operatorTextColor}
              >
                {task.operator}
              </Text>
            )}
          </>
        )}
      </Flex>
    </Tooltip>
  );
};

export default DagNode;
