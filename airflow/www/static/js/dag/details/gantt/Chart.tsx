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
import { Box, Text, Tooltip, Flex } from "@chakra-ui/react";
import useSelection from "src/dag/useSelection";
import { getDuration, formatDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import { useContainerRef } from "src/context/containerRef";
import { hoverDelay } from "src/utils";
import Time from "src/components/Time";
import type { DagRun, Task } from "src/types";

interface Props {
  ganttWidth?: number;
  openGroupIds: string[];
  dagRun: DagRun;
  tasks: Task[];
}

const Chart = ({ ganttWidth = 500, openGroupIds, tasks, dagRun }: Props) => {
  const {
    selected: { runId, taskId },
    onSelect,
  } = useSelection();
  const containerRef = useContainerRef();

  const runDuration = getDuration(dagRun?.startDate, dagRun?.endDate);

  return (
    <div>
      {tasks?.map((task) => {
        const instance = task.instances.find((ti) => ti.runId === runId);
        const isSelected = taskId === instance?.taskId;
        const hasQueuedDttm = !!instance?.queuedDttm;
        const isOpen = openGroupIds.includes(task.label || "");
        const isGroup = !!task.children;

        // Calculate durations in ms
        const taskDuration = getDuration(
          instance?.startDate,
          instance?.endDate
        );
        const queuedDuration = hasQueuedDttm
          ? getDuration(instance?.queuedDttm, instance?.startDate)
          : 0;
        const taskStartOffset = getDuration(
          dagRun.startDate,
          instance?.queuedDttm || instance?.startDate
        );

        // Percent of each duration vs the overall dag run
        const taskDurationPercent = taskDuration / runDuration;
        const taskStartOffsetPercent = taskStartOffset / runDuration;
        const queuedDurationPercent = queuedDuration / runDuration;

        // Calculate the pixel width of the queued and task bars and the position in the graph
        // Min width should be 5px
        let width = ganttWidth * taskDurationPercent;
        if (width < 5) width = 5;
        let queuedWidth = hasQueuedDttm
          ? ganttWidth * queuedDurationPercent
          : 0;
        if (hasQueuedDttm && queuedWidth < 5) queuedWidth = 5;
        const offsetMargin = taskStartOffsetPercent * ganttWidth;

        return (
          <div key={`gantt-${task.id}`}>
            <Box
              py="4px"
              borderBottomWidth={1}
              borderBottomColor={isGroup && isOpen ? "gray.400" : "gray.200"}
              bg={isSelected ? "blue.100" : "inherit"}
            >
              {instance ? (
                <Tooltip
                  label={
                    <Box>
                      <Text>
                        Task{isGroup ? " Group" : ""}: {task.label}
                      </Text>
                      <br />
                      {hasQueuedDttm && (
                        <Text>
                          Queued Duration: {formatDuration(queuedDuration)}
                        </Text>
                      )}
                      <Text>Run Duration: {formatDuration(taskDuration)}</Text>
                      <br />
                      {hasQueuedDttm && (
                        <Text>
                          Queued At: <Time dateTime={instance?.queuedDttm} />
                        </Text>
                      )}
                      <Text>
                        Start: <Time dateTime={instance?.startDate} />
                      </Text>
                      <Text>
                        {instance?.endDate ? (
                          <>
                            End: <Time dateTime={instance?.endDate} />
                          </>
                        ) : (
                          "Ongoing"
                        )}
                      </Text>
                    </Box>
                  }
                  hasArrow
                  portalProps={{ containerRef }}
                  placement="top"
                  openDelay={hoverDelay}
                >
                  <Flex
                    width={`${width + queuedWidth}px`}
                    cursor="pointer"
                    pointerEvents="auto"
                    marginLeft={`${offsetMargin}px`}
                    onClick={() =>
                      onSelect({
                        runId: instance.runId,
                        taskId: instance.taskId,
                      })
                    }
                  >
                    {instance.state !== "queued" && hasQueuedDttm && (
                      <SimpleStatus
                        state="queued"
                        width={`${queuedWidth}px`}
                        borderRightRadius={0}
                        // The normal queued color is too dark when next to the actual task's state
                        opacity={0.6}
                      />
                    )}
                    <SimpleStatus
                      state={instance.state}
                      width={`${width}px`}
                      borderLeftRadius={
                        instance.state !== "queued" && hasQueuedDttm
                          ? 0
                          : undefined
                      }
                    />
                  </Flex>
                </Tooltip>
              ) : (
                <Box height="10px" />
              )}
            </Box>
            {isOpen && !!task.children && (
              <Chart
                ganttWidth={ganttWidth}
                openGroupIds={openGroupIds}
                dagRun={dagRun}
                tasks={task.children}
              />
            )}
          </div>
        );
      })}
    </div>
  );
};

export default Chart;
