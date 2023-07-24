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
import { Box, Text } from "@chakra-ui/react";
import { getDuration, formatDuration } from "src/datetime_utils";
import Time from "src/components/Time";
import type { Task, TaskInstance } from "src/types";

interface Props {
  instance: TaskInstance;
  task: Task;
}

const GanttTooltip = ({ task, instance }: Props) => {
  const hasQueuedDttm = !!instance?.queuedDttm;
  const isGroup = !!task.children;
  const isMappedOrGroupSummary = isGroup || task.isMapped;

  // Calculate durations in ms
  const taskDuration = getDuration(instance?.startDate, instance?.endDate);
  const queuedDuration = hasQueuedDttm
    ? getDuration(instance?.queuedDttm, instance?.startDate)
    : 0;
  return (
    <Box>
      <Text>
        Task{isGroup ? " Group" : ""}: {task.label}
      </Text>
      <br />
      {hasQueuedDttm && (
        <Text>
          {isMappedOrGroupSummary && "Total "}Queued Duration:{" "}
          {formatDuration(queuedDuration)}
        </Text>
      )}
      <Text>
        {isMappedOrGroupSummary && "Total "}Run Duration:{" "}
        {formatDuration(taskDuration)}
      </Text>
      <br />
      {hasQueuedDttm && (
        <Text>
          {isMappedOrGroupSummary && "Earliest "}Queued At:{" "}
          <Time dateTime={instance?.queuedDttm} />
        </Text>
      )}
      <Text>
        {isMappedOrGroupSummary && "Earliest "}Start:{" "}
        <Time dateTime={instance?.startDate} />
      </Text>
      <Text>
        {instance?.endDate ? (
          <>
            {isMappedOrGroupSummary && "Latest "}End:{" "}
            <Time dateTime={instance?.endDate} />
          </>
        ) : (
          "Ongoing"
        )}
      </Text>
    </Box>
  );
};

export default GanttTooltip;
