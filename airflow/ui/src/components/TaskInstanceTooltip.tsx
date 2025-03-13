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
import { Box, Text } from "@chakra-ui/react";

import type {
  TaskInstanceHistoryResponse,
  TaskInstanceResponse,
  GridTaskInstanceSummary,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip, type TooltipProps } from "src/components/ui";

type Props = {
  readonly taskInstance?: GridTaskInstanceSummary | TaskInstanceHistoryResponse | TaskInstanceResponse;
} & Omit<TooltipProps, "content">;

const TaskInstanceTooltip = ({ children, positioning, taskInstance, ...rest }: Props) =>
  taskInstance === undefined ? (
    children
  ) : (
    <Tooltip
      {...rest}
      content={
        <Box>
          <Text>State: {taskInstance.state}</Text>
          {"dag_run_id" in taskInstance ? <Text>Run ID: {taskInstance.dag_run_id}</Text> : undefined}
          <Text>
            Start Date: <Time datetime={taskInstance.start_date} />
          </Text>
          <Text>
            End Date: <Time datetime={taskInstance.end_date} />
          </Text>
          {taskInstance.try_number > 1 && <Text>Try Number: {taskInstance.try_number}</Text>}
          {"duration" in taskInstance ? (
            <Text>Duration: {taskInstance.duration?.toFixed(2) ?? 0}s</Text>
          ) : undefined}
        </Box>
      }
      key={taskInstance.task_id}
      portalled
      positioning={{
        offset: {
          crossAxis: 5,
          mainAxis: 5,
        },
        placement: "bottom-start",
        ...positioning,
      }}
    >
      {children}
    </Tooltip>
  );

export default TaskInstanceTooltip;
