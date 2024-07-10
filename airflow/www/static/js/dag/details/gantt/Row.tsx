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
import { Box } from "@chakra-ui/react";
import useSelection from "src/dag/useSelection";
import { boxSize } from "src/dag/StatusBox";
import { getMetaValue } from "src/utils";
import type { Task } from "src/types";
import { useTIHistory } from "src/api";
import InstanceBar from "./InstanceBar";

interface Props {
  ganttWidth?: number;
  openGroupIds: string[];
  task: Task;
  ganttStartDate?: string | null;
  ganttEndDate?: string | null;
}

const dagId = getMetaValue("dag_id");

const Row = ({
  ganttWidth = 500,
  openGroupIds,
  task,
  ganttStartDate,
  ganttEndDate,
}: Props) => {
  const {
    selected: { runId, taskId },
  } = useSelection();

  const instance = task.instances.find((ti) => ti.runId === runId);

  const { data: tiHistory } = useTIHistory({
    dagId,
    taskId: task.id || "",
    runId: runId || "",
    enabled: !!(instance?.tryNumber && instance?.tryNumber > 1) && !!task.id, // Only try to look up task tries if try number > 1
  });

  const isSelected = taskId === instance?.taskId;
  const isOpen = openGroupIds.includes(task.id || "");

  return (
    <div>
      <Box
        py="4px"
        borderBottomWidth={1}
        borderBottomColor={!!task.children && isOpen ? "gray.400" : "gray.200"}
        bg={isSelected ? "blue.100" : "inherit"}
        position="relative"
        width={ganttWidth}
        height={`${boxSize + 9}px`}
      >
        {!!instance && (
          <InstanceBar
            instance={{
              ...instance,
              queuedWhen: instance.queuedDttm,
              dagRunId: instance.runId,
            }}
            task={task}
            ganttWidth={ganttWidth}
            ganttStartDate={ganttStartDate}
            ganttEndDate={ganttEndDate}
          />
        )}
        {(tiHistory || []).map((ti) => (
          <InstanceBar
            key={`${taskId}-${ti.tryNumber}`}
            instance={ti}
            task={task}
            ganttWidth={ganttWidth}
            ganttStartDate={ganttStartDate}
            ganttEndDate={ganttEndDate}
          />
        ))}
      </Box>
      {isOpen &&
        !!task.children &&
        task.children.map((c) => (
          <Row
            ganttWidth={ganttWidth}
            openGroupIds={openGroupIds}
            ganttStartDate={ganttStartDate}
            ganttEndDate={ganttEndDate}
            task={c}
            key={`gantt-${c.id}`}
          />
        ))}
    </div>
  );
};

export default Row;
