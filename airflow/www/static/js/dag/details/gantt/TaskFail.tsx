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
import { Box, Tooltip, Text } from "@chakra-ui/react";

import { getDuration } from "src/datetime_utils";
import { SimpleStatus } from "src/dag/StatusBox";
import { useContainerRef } from "src/context/containerRef";
import { hoverDelay } from "src/utils";
import Time from "src/components/Time";

import type { TaskFail as TaskFailType } from "src/api/useTaskFails";

interface Props {
  taskFail: TaskFailType;
  runDuration: number;
  ganttWidth: number;
  ganttStartDate?: string | null;
}

const TaskFail = ({
  taskFail,
  runDuration,
  ganttWidth,
  ganttStartDate,
}: Props) => {
  const containerRef = useContainerRef();

  const duration = getDuration(taskFail?.startDate, taskFail?.endDate);
  const percent = duration / runDuration;
  const failWidth = ganttWidth * percent;

  const startOffset = getDuration(ganttStartDate, taskFail?.startDate);
  const offsetLeft = (startOffset / runDuration) * ganttWidth;

  return (
    <Tooltip
      label={
        <Box>
          <Text mb={2}>Task Fail</Text>
          {taskFail?.startDate && (
            <Text>
              Start: <Time dateTime={taskFail?.startDate} />
            </Text>
          )}
          {taskFail?.endDate && (
            <Text>
              End: <Time dateTime={taskFail?.endDate} />
            </Text>
          )}
          <Text mt={2} fontSize="sm">
            Can only show previous Task Fails, other tries are not yet saved.
          </Text>
        </Box>
      }
      hasArrow
      portalProps={{ containerRef }}
      placement="top"
      openDelay={hoverDelay}
      top="4px"
    >
      <Box
        position="absolute"
        left={`${offsetLeft}px`}
        cursor="pointer"
        top="4px"
      >
        <SimpleStatus state="failed" width={`${failWidth}px`} />
      </Box>
    </Tooltip>
  );
};

export default TaskFail;
