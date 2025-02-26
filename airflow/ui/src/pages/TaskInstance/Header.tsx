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
import { useRef } from "react";
import { FiMessageSquare } from "react-icons/fi";
import { MdOutlineTask } from "react-icons/md";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import DisplayMarkdownButton from "src/components/DisplayMarkdownButton";
import { HeaderCard } from "src/components/HeaderCard";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import Time from "src/components/Time";
import { getDuration, useContainerWidth } from "src/utils";

export const Header = ({
  isRefreshing,
  taskInstance,
}: {
  readonly isRefreshing?: boolean;
  readonly taskInstance: TaskInstanceResponse;
}) => {
  const containerRef = useRef<HTMLDivElement>();
  const containerWidth = useContainerWidth(containerRef);

  const stats = [
    { label: "Operator", value: taskInstance.operator },
    ...(taskInstance.map_index > -1 ? [{ label: "Map Index", value: taskInstance.rendered_map_index }] : []),
    ...(taskInstance.try_number > 1 ? [{ label: "Try Number", value: taskInstance.try_number }] : []),
    { label: "Start", value: <Time datetime={taskInstance.start_date} /> },
    { label: "End", value: <Time datetime={taskInstance.end_date} /> },
    { label: "Duration", value: `${getDuration(taskInstance.start_date, taskInstance.end_date)}s` },
    ...(taskInstance.dag_version?.version_number === undefined
      ? []
      : [{ label: "Dag Version", value: `v${taskInstance.dag_version.version_number}` }]),
  ];

  return (
    <Box ref={containerRef}>
      <HeaderCard
        actions={
          <>
            {taskInstance.note === null || taskInstance.note.length === 0 ? undefined : (
              <DisplayMarkdownButton
                header="Task Instance Note"
                icon={<FiMessageSquare color="black" />}
                mdContent={taskInstance.note}
                text="Note"
              />
            )}
            <ClearTaskInstanceButton taskInstance={taskInstance} withText={containerWidth > 700} />
            <MarkTaskInstanceAsButton taskInstance={taskInstance} withText={containerWidth > 700} />
          </>
        }
        icon={<MdOutlineTask />}
        isRefreshing={isRefreshing}
        state={taskInstance.state}
        stats={stats}
        subTitle={<Time datetime={taskInstance.start_date} />}
        title={`${taskInstance.task_display_name}${taskInstance.map_index > -1 ? ` [${taskInstance.rendered_map_index ?? taskInstance.map_index}]` : ""}`}
      />
    </Box>
  );
};
