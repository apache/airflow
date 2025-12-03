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
import { useTranslation } from "react-i18next";

import type {
  LightGridTaskInstanceSummary,
  TaskInstanceHistoryResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip, type TooltipProps } from "src/components/ui";
import { renderDuration } from "src/utils";

type Props = {
  readonly taskInstance?: LightGridTaskInstanceSummary | TaskInstanceHistoryResponse | TaskInstanceResponse;
} & Omit<TooltipProps, "content">;

const TaskInstanceTooltip = ({ children, positioning, taskInstance, ...rest }: Props) => {
  const { t: translate } = useTranslation("common");

  return taskInstance === undefined ? (
    children
  ) : (
    <Tooltip
      {...rest}
      content={
        <Box>
          <Text>
            {translate("state")}:{" "}
            {taskInstance.state
              ? translate(`common:states.${taskInstance.state}`)
              : translate("common:states.no_status")}
          </Text>
          {"dag_run_id" in taskInstance ? (
            <Text>
              {translate("runId")}: {taskInstance.dag_run_id}
            </Text>
          ) : undefined}
          {"start_date" in taskInstance ? (
            <>
              {taskInstance.try_number > 1 && (
                <Text>
                  {translate("tryNumber")}: {taskInstance.try_number}
                </Text>
              )}
              <Text>
                {translate("startDate")}: <Time datetime={taskInstance.start_date} />
              </Text>
              <Text>
                {translate("endDate")}: <Time datetime={taskInstance.end_date} />
              </Text>
              <Text>
                {translate("duration")}: {renderDuration(taskInstance.duration)}
              </Text>
            </>
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
};

export default TaskInstanceTooltip;
