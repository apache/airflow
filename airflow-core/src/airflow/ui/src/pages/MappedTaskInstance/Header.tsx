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
import { Box } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { MdOutlineTask } from "react-icons/md";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { HeaderCard } from "src/components/HeaderCard";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({ taskInstance }: { readonly taskInstance: LightGridTaskInstanceSummary }) => {
  const { t: translate } = useTranslation();
  const entries: Array<{ label: string; value: number | ReactNode | string }> = [];
  let taskCount: number = 0;

  Object.entries(taskInstance.child_states ?? {}).forEach(([taskState, count]) => {
    entries.push({
      label: translate("total", { state: translate(`states.${taskState.toLowerCase()}`) }),
      value: count,
    });
    taskCount += count;
  });
  const stats = [
    ...entries,
    { label: translate("startDate"), value: <Time datetime={taskInstance.min_start_date} /> },
    { label: translate("endDate"), value: <Time datetime={taskInstance.max_end_date} /> },
    ...(Boolean(taskInstance.max_end_date)
      ? [
          {
            label: translate("duration"),
            value: getDuration(taskInstance.min_start_date, taskInstance.max_end_date),
          },
        ]
      : []),
  ];

  return (
    <Box>
      <HeaderCard
        icon={<MdOutlineTask />}
        state={taskInstance.state}
        stats={stats}
        subTitle={<Time datetime={taskInstance.min_start_date} />}
        title={`${taskInstance.task_id} [${taskCount}]`}
      />
    </Box>
  );
};
