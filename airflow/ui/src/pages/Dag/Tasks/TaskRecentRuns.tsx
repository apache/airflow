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
import { Box, Flex } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import { Link } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { getTaskInstanceLink } from "src/utils/links";

dayjs.extend(duration);

const BAR_HEIGHT = 60;

export const TaskRecentRuns = ({
  taskInstances,
}: {
  readonly taskInstances: Array<TaskInstanceResponse>;
}) => {
  if (!taskInstances.length) {
    return undefined;
  }

  const taskInstancesWithDuration = taskInstances.map((taskInstance) => ({
    ...taskInstance,
    duration:
      dayjs.duration(dayjs(taskInstance.end_date ?? dayjs()).diff(taskInstance.start_date)).asSeconds() || 0,
  }));

  const max = Math.max.apply(
    undefined,
    taskInstancesWithDuration.map((taskInstance) => taskInstance.duration),
  );

  return (
    <Flex alignItems="flex-end" flexDirection="row-reverse">
      {taskInstancesWithDuration.map((taskInstance) => (
        <TaskInstanceTooltip key={taskInstance.dag_run_id} taskInstance={taskInstance}>
          <Link to={getTaskInstanceLink(taskInstance)}>
            <Box p={1}>
              <Box
                bg={`${taskInstance.state ?? "none"}.solid`}
                borderRadius="4px"
                height={`${(taskInstance.duration / max) * BAR_HEIGHT}px`}
                minHeight={1}
                width="4px"
              />
            </Box>
          </Link>
        </TaskInstanceTooltip>
      ))}
    </Flex>
  );
};
