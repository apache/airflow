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
import { Box, Flex, Text } from "@chakra-ui/react";
import type { NodeProps } from "reactflow";

import { SimpleStatus } from "src/dag/StatusBox";
import useSelection from "src/dag/useSelection";
import { getGroupAndMapSummary, hoverDelay } from "src/utils";
import Tooltip from "src/components/Tooltip";
import InstanceTooltip from "src/dag/InstanceTooltip";
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
    onToggleCollapse,
    isOpen,
    isActive,
    setupTeardownType,
    labelStyle,
    style,
    isZoomedOut,
  },
}: NodeProps<CustomNodeProps>) => {
  const { onSelect } = useSelection();
  const containerRef = useContainerRef();

  if (!task) return null;

  const bg = isOpen ? "blackAlpha.50" : "white";
  const { isMapped } = task;
  const mappedStates = instance?.mappedStates;

  const { totalTasks } = getGroupAndMapSummary({ group: task, mappedStates });

  const taskName = isMapped
    ? `${label} [${instance ? totalTasks : " "}]`
    : label;

  let operatorTextColor = "";
  let operatorBG = "";
  if (style) {
    [, operatorBG] = style.split(":");
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
      <Box
        borderRadius={isZoomedOut ? 10 : 5}
        borderWidth={(isSelected ? 4 : 2) * (isZoomedOut ? 3 : 1)}
        borderColor={nodeBorderColor}
        bg={
          !task.children?.length && operatorBG
            ? // Fade the operator color to clash less with the task instance status
              `color-mix(in srgb, ${operatorBG.replace(";", "")} 80%, white)`
            : bg
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
      >
        <TaskName
          label={taskName}
          isOpen={isOpen}
          isGroup={!!childCount}
          onClick={(e) => {
            e.stopPropagation();
            onToggleCollapse();
          }}
          setupTeardownType={setupTeardownType}
          fontWeight="bold"
          isZoomedOut={isZoomedOut}
          mt={isZoomedOut ? -2 : 0}
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
      </Box>
    </Tooltip>
  );
};

export default DagNode;
