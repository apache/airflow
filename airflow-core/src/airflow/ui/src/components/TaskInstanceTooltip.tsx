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
import { getDuration } from "src/utils";

type TIUnion = LightGridTaskInstanceSummary | TaskInstanceHistoryResponse | TaskInstanceResponse;

type Props = {
  readonly taskInstance?: TIUnion;
} & Omit<TooltipProps, "content">;

/** Full TI shapes carry start/end; grid light shape carries min/max. */
const hasStartEnd = (
  ti: TIUnion,
): ti is Extract<TIUnion, TaskInstanceHistoryResponse | TaskInstanceResponse> => "start_date" in ti;

const hasMinMax = (
  ti: TIUnion,
): ti is { max_end_date: string | null; min_start_date: string | null } & LightGridTaskInstanceSummary =>
  "min_start_date" in ti && "max_end_date" in ti;

const normalize = (value: string | null | undefined): string | undefined => value ?? undefined;

const TaskInstanceTooltip = ({ children, positioning, taskInstance, ...rest }: Props) => {
  const { t: translate } = useTranslation("common");

  if (taskInstance === undefined) {
    return children;
  }

  const taskId =
    "task_id" in taskInstance && typeof taskInstance.task_id === "string" ? taskInstance.task_id : undefined;
  const state = "state" in taskInstance ? taskInstance.state : undefined;
  const triggerRule =
    "trigger_rule" in taskInstance && typeof taskInstance.trigger_rule === "string"
      ? taskInstance.trigger_rule
      : undefined;

  // Computing timing + duration with correct fallbacks
  let startedIso: string | undefined;
  let endedIso: string | undefined;

  if (hasStartEnd(taskInstance)) {
    startedIso = normalize(taskInstance.start_date);
    endedIso = normalize(taskInstance.end_date);
  } else if (hasMinMax(taskInstance)) {
    startedIso = normalize(taskInstance.min_start_date);
    endedIso = normalize(taskInstance.max_end_date);
  } else {
    startedIso = undefined;
    endedIso = undefined;
  }

  const hasStarted = typeof startedIso === "string" && startedIso.length > 0;
  const hasEnded = typeof endedIso === "string" && endedIso.length > 0;

  return (
    <Tooltip
      {...rest}
      content={
        <Box>
          {taskId === undefined ? undefined : (
            <Text>
              {translate("taskId")}: {taskId}
            </Text>
          )}

          {state === undefined ? undefined : (
            <Text>
              {translate("state")}: {state}
            </Text>
          )}

          {hasStartEnd(taskInstance) && taskInstance.try_number > 1 ? (
            <Text>
              {translate("tryNumber")}: {taskInstance.try_number}
            </Text>
          ) : undefined}

          {hasStarted ? (
            <Text>
              {translate("startDate")}: <Time datetime={startedIso} />
            </Text>
          ) : undefined}

          {hasEnded ? (
            <Text>
              {translate("endDate")}: <Time datetime={endedIso} />
            </Text>
          ) : undefined}

          {hasStarted && hasEnded ? (
            <Text>
              {translate("duration")}: {getDuration(startedIso, endedIso)}
            </Text>
          ) : undefined}

          {triggerRule === undefined ? undefined : (
            <Text>
              {translate("task.triggerRule")}: {triggerRule}
            </Text>
          )}
        </Box>
      }
      key={taskId ?? "ti"}
      portalled
      positioning={{
        offset: { crossAxis: 5, mainAxis: 5 },
        placement: "bottom-start",
        ...positioning,
      }}
    >
      {children}
    </Tooltip>
  );
};

export default TaskInstanceTooltip;
