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
import { snakeCase } from "lodash";

import { getGroupAndMapSummary } from "src/utils";
import { formatDuration, getDuration } from "src/datetime_utils";
import type { TaskInstance, Task } from "src/types";
import Time from "src/components/Time";

interface Props {
  group: Task;
  instance: TaskInstance;
}

const InstanceTooltip = ({
  group,
  instance: { taskId, startDate, endDate, state, runId, mappedStates, note },
}: Props) => {
  if (!group) return null;
  const isGroup = !!group.children;
  const { isMapped } = group;
  const summary: React.ReactNode[] = [];

  const { totalTasks, childTaskMap } = getGroupAndMapSummary({
    group,
    runId,
    mappedStates,
  });

  childTaskMap.forEach((key, val) => {
    const childState = snakeCase(val);
    if (key > 0) {
      summary.push(
        <Text key={childState} ml="10px">
          {childState}
          {": "}
          {key}
        </Text>
      );
    }
  });

  return (
    <Box py="2px">
      <Text>Task Id: {taskId}</Text>
      {group.tooltip && <Text>{group.tooltip}</Text>}
      {isMapped && totalTasks > 0 && (
        <Text>
          {totalTasks} mapped task
          {isGroup && " group"}
          {totalTasks > 1 && "s"}
        </Text>
      )}
      <Text>
        {isGroup || totalTasks ? "Overall " : ""}
        Status: {state || "no status"}
      </Text>
      {(isGroup || isMapped) && summary}
      {startDate && (
        <>
          <Text>
            Started: <Time dateTime={startDate} />
          </Text>
          <Text>
            Duration: {formatDuration(getDuration(startDate, endDate))}
          </Text>
        </>
      )}
      {group.triggerRule && <Text>Trigger Rule: {group.triggerRule}</Text>}
      {note && <Text>Contains a note</Text>}
    </Box>
  );
};

export default InstanceTooltip;
