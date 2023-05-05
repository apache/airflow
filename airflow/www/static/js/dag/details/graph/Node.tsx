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
import { Box, Text, Flex } from "@chakra-ui/react";
import { Handle, NodeProps, Position } from "reactflow";

import { SimpleStatus } from "src/dag/StatusBox";
import useSelection from "src/dag/useSelection";
import type { DagRun, Task, TaskInstance } from "src/types";
import { getGroupAndMapSummary, hoverDelay } from "src/utils";
import Tooltip from "src/components/Tooltip";
import InstanceTooltip from "src/dag/InstanceTooltip";
import { useContainerRef } from "src/context/containerRef";

export interface CustomNodeProps {
  label: string;
  height?: number;
  width?: number;
  isJoinNode?: boolean;
  instance?: TaskInstance;
  task?: Task | null;
  isSelected: boolean;
  latestDagRunId: DagRun["runId"];
  childCount?: number;
  onToggleCollapse: () => void;
  isOpen?: boolean;
  isActive?: boolean;
}

export const BaseNode = ({
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
  },
}: NodeProps<CustomNodeProps>) => {
  const { onSelect } = useSelection();
  const containerRef = useContainerRef();

  if (!task) return null;

  const { isMapped } = task;
  const mappedStates = instance?.mappedStates;

  const { totalTasks } = getGroupAndMapSummary({ group: task, mappedStates });

  const taskName = isMapped
    ? `${label} [${instance ? totalTasks : " "}]`
    : label;

  const bg = isOpen ? "blackAlpha.50" : "white";

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
        borderRadius={5}
        borderWidth={1}
        borderColor={isSelected ? "blue.400" : "gray.400"}
        bg={isSelected ? "blue.50" : bg}
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
      >
        <Flex
          justifyContent="space-between"
          width={width}
          p={2}
          flexWrap="wrap"
        >
          <Flex flexDirection="column">
            <Text noOfLines={1} maxWidth={`calc(${width}px - 8px)`}>
              {taskName}
            </Text>
            {!!instance && instance.state && (
              <Flex alignItems="center">
                <SimpleStatus state={instance.state} />
                <Text ml={2} color="gray.500" fontSize="sm">
                  {instance.state}
                </Text>
              </Flex>
            )}
            {task?.operator && (
              <Text color="gray.500" fontWeight={400} fontSize="md">
                {task.operator}
              </Text>
            )}
          </Flex>
          {!!childCount && (
            <Text
              color="blue.600"
              cursor="pointer"
              // Increase the target area to expand/collapse a group
              p={3}
              m={-3}
              onClick={(e) => {
                e.stopPropagation();
                onToggleCollapse();
              }}
            >
              {isOpen ? "- " : "+ "}
              {childCount} tasks
            </Text>
          )}
        </Flex>
      </Box>
    </Tooltip>
  );
};

const Node = (props: NodeProps<CustomNodeProps>) => {
  const {
    data: { height, width, isJoinNode, task },
  } = props;
  if (isJoinNode) {
    return (
      <>
        <Handle
          type="target"
          position={Position.Top}
          style={{ visibility: "hidden" }}
        />
        <Box
          height={`${height}px`}
          width={`${width}px`}
          borderRadius={width}
          bg="gray.400"
        />
        <Handle
          type="source"
          position={Position.Bottom}
          style={{ visibility: "hidden" }}
        />
      </>
    );
  }

  if (!task) return null;
  return (
    <>
      <Handle
        type="target"
        position={Position.Top}
        style={{ visibility: "hidden" }}
      />
      <BaseNode {...props} />
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ visibility: "hidden" }}
      />
    </>
  );
};

export default Node;
