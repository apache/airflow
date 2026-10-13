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
import type { ReactNode } from "react";

import { Box, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdOutlineTask } from "react-icons/md";
import { useParams, useSearchParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { ClearTaskInstanceButton } from "src/components/Clear";
import { HeaderCard } from "src/components/HeaderCard";
import Time from "src/components/Time";

import { selectActivePass, useLoopFilterOptions } from "src/queries/useLoopFilterOptions";
import { formatNumber, useDurationFormat } from "src/utils";

export const Header = ({ taskInstance }: { readonly taskInstance: LightGridTaskInstanceSummary }) => {
  const { dagId = "", runId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const { i18n, t: translate } = useTranslation();
  // These expansions were produced by one pass of the enclosing loop, and the summary they are
  // built from does not record which, so it comes from the scope that addressed them.
  const { options: loopOptions } = useLoopFilterOptions({ dagId, groupId: taskInstance.task_id, runId });
  const pass = selectActivePass(loopOptions, searchParams);
  const { formatElapsed } = useDurationFormat();
  const entries: Array<{ key?: string; label: string; value: number | ReactNode | string }> = [];
  let taskCount: number = 0;

  Object.entries(taskInstance.child_states ?? {}).forEach(([state, count]) => {
    entries.push({
      key: state,
      label: translate("total", { state: translate(`states.${state}`) }),
      value: (
        <HStack gap={2}>
          <Box
            bg={`${state}.solid`}
            border="1px solid"
            borderColor="border.emphasized"
            borderRadius="2px"
            height="10px"
            width="10px"
          />
          {formatNumber(count, i18n.language)}
        </HStack>
      ),
    });
    taskCount += count;
  });
  const stats = [
    ...entries,
    ...(pass === undefined
      ? []
      : [
          {
            label: translate("common:filters.iteration"),
            value: `${pass.iteration}/${loopOptions[0]?.iterationsRan ?? pass.iteration}`,
          },
        ]),
    { label: translate("startDate"), value: <Time datetime={taskInstance.min_start_date} /> },
    { label: translate("endDate"), value: <Time datetime={taskInstance.max_end_date} /> },
    ...(Boolean(taskInstance.max_end_date)
      ? [
          {
            label: translate("duration"),
            value: formatElapsed(taskInstance.min_start_date, taskInstance.max_end_date),
          },
        ]
      : []),
  ];

  return (
    <HeaderCard
      actions={
        <ClearTaskInstanceButton
          allMapped
          bg="bg"
          dagId={dagId}
          dagRunId={runId}
          isHotkeyEnabled
          taskId={taskInstance.task_id}
          variant="outline"
        />
      }
      icon={<MdOutlineTask />}
      state={taskInstance.state}
      stats={stats}
      subTitle={<Time datetime={taskInstance.min_start_date} />}
      title={`${taskInstance.task_display_name} [${formatNumber(taskCount, i18n.language)}]`}
      type="taskInstance"
    />
  );
};
