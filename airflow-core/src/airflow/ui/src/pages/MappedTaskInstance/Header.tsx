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

import type { GridTaskInstanceSummary } from "openapi/requests/types.gen";
import { HeaderCard } from "src/components/HeaderCard";
import Time from "src/components/Time";
import { getDuration } from "src/utils";

export const Header = ({
  isRefreshing,
  taskInstance,
}: {
  readonly isRefreshing?: boolean;
  readonly taskInstance: GridTaskInstanceSummary;
}) => {
  const entries: Array<{ label: string; value: number | ReactNode | string }> = [];

  if (taskInstance.child_states !== null) {
    Object.entries(taskInstance.child_states).forEach(([state, count]) => {
      if (count > 0) {
        entries.push({ label: `Total ${state}`, value: count });
      }
    });
  }
  const stats = [
    ...entries,
    { label: "Start", value: <Time datetime={taskInstance.start_date} /> },
    { label: "End", value: <Time datetime={taskInstance.end_date} /> },
    ...(Boolean(taskInstance.start_date)
      ? [{ label: "Duration", value: `${getDuration(taskInstance.start_date, taskInstance.end_date)}s` }]
      : []),
  ];

  return (
    <Box>
      <HeaderCard
        icon={<MdOutlineTask />}
        isRefreshing={isRefreshing}
        state={taskInstance.state}
        stats={stats}
        subTitle={<Time datetime={taskInstance.start_date} />}
        title={`${taskInstance.task_id} [${taskInstance.task_count}]`}
      />
    </Box>
  );
};
