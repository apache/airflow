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
import { Tooltip, Flex } from "@chakra-ui/react";
import useSelection from "src/dag/useSelection";
import { getDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import { useContainerRef } from "src/context/containerRef";
import { hoverDelay } from "src/utils";
import type { Task } from "src/types";
import type { TaskInstance } from "src/types/api-generated";
import GanttTooltip from "./GanttTooltip";

type Instance = Pick<
  TaskInstance,
  | "startDate"
  | "endDate"
  | "tryNumber"
  | "queuedWhen"
  | "dagRunId"
  | "state"
  | "taskId"
>;

interface Props {
  ganttWidth?: number;
  task: Task;
  instance: Instance;
  ganttStartDate?: string | null;
  ganttEndDate?: string | null;
}

const InstanceBar = ({
  ganttWidth = 500,
  task,
  instance,
  ganttStartDate,
  ganttEndDate,
}: Props) => {
  const { onSelect } = useSelection();
  const containerRef = useContainerRef();

  const runDuration = getDuration(ganttStartDate, ganttEndDate);
  const { queuedWhen } = instance;

  const hasValidQueuedDttm =
    !!queuedWhen &&
    (instance?.startDate && queuedWhen
      ? queuedWhen < instance.startDate
      : true);

  // Calculate durations in ms
  const taskDuration = getDuration(instance?.startDate, instance?.endDate);
  const queuedDuration = hasValidQueuedDttm
    ? getDuration(queuedWhen, instance?.startDate)
    : 0;
  const taskStartOffset = hasValidQueuedDttm
    ? getDuration(ganttStartDate, queuedWhen || instance?.startDate)
    : getDuration(ganttStartDate, instance?.startDate);

  // Percent of each duration vs the overall dag run
  const taskDurationPercent = taskDuration / runDuration;
  const taskStartOffsetPercent = taskStartOffset / runDuration;
  const queuedDurationPercent = queuedDuration / runDuration;

  // Calculate the pixel width of the queued and task bars and the position in the graph
  // Min width should be 5px
  let width = ganttWidth * taskDurationPercent;
  if (width < 5) width = 5;
  let queuedWidth = hasValidQueuedDttm ? ganttWidth * queuedDurationPercent : 0;
  if (hasValidQueuedDttm && queuedWidth < 5) queuedWidth = 5;
  const offsetMargin = taskStartOffsetPercent * ganttWidth;

  if (!instance) return null;

  return (
    <Tooltip
      label={<GanttTooltip task={task} instance={instance} />}
      hasArrow
      portalProps={{ containerRef }}
      placement="top"
      openDelay={hoverDelay}
    >
      <Flex
        position="absolute"
        top="4px"
        left={`${offsetMargin}px`}
        transition="left 0.5s"
        cursor="pointer"
        pointerEvents="auto"
        onClick={() => {
          onSelect({
            runId: instance.dagRunId,
            taskId: instance.taskId,
          });
        }}
      >
        {instance.state !== "queued" && hasValidQueuedDttm && (
          <SimpleStatus
            state="queued"
            width={`${queuedWidth}px`}
            borderRightRadius={0}
            transition="width 0.5s"
            // The normal queued color is too dark when next to the actual task's state
            opacity={0.6}
          />
        )}
        <SimpleStatus
          state={
            !instance.state || instance?.state === "none"
              ? null
              : instance.state
          }
          width={`${width}px`}
          transition="width 0.5s"
          borderLeftRadius={
            instance.state !== "queued" && hasValidQueuedDttm ? 0 : undefined
          }
        />
      </Flex>
    </Tooltip>
  );
};

export default InstanceBar;
