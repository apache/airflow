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
import { MdOutlineTask } from "react-icons/md";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { HeaderCard } from "src/components/HeaderCard";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({
  isRefreshing,
  taskInstance,
}: {
  readonly isRefreshing?: boolean;
  readonly taskInstance: LightGridTaskInstanceSummary;
}) => {
  const entries: Array<{ label: string; value: number | ReactNode | string }> = [];

  Object.entries(taskInstance.child_states ?? {}).forEach(([state, count]) => {
    entries.push({ label: `Total ${state}`, value: count });
  });
  const stats = [
    ...entries,
    { label: "Earliest Start", value: <Time datetime={taskInstance.min_start_date} /> },
    { label: "Latest End", value: <Time datetime={taskInstance.max_end_date} /> },
    ...(Boolean(taskInstance.max_end_date)
      ? [
          {
            label: "Total Duration",
            value: getDuration(taskInstance.min_start_date, taskInstance.max_end_date),
          },
        ]
      : []),
  ];

  return (
    <Box>
      <HeaderCard
        icon={<MdOutlineTask />}
        isRefreshing={isRefreshing}
        state={taskInstance.state}
        stats={stats}
        subTitle={<Time datetime={taskInstance.min_start_date} />}
        title={taskInstance.task_id}
      />
    </Box>
  );
};
