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

import React, { useRef } from "react";
import { Box } from "@chakra-ui/react";

import { useGridData, useTaskInstance } from "src/api";
import { getMetaValue, getTask, useOffsetTop } from "src/utils";
import type { DagRun, TaskInstance as GridTaskInstance } from "src/types";
import NotesAccordion from "src/dag/details/NotesAccordion";

import TaskNav from "./Nav";
import ExtraLinks from "./ExtraLinks";
import Details from "./Details";
import AssetUpdateEvents from "./AssetUpdateEvents";
import TriggererInfo from "./TriggererInfo";
import TaskFailedDependency from "./TaskFailedDependency";
import TaskDocumentation from "./TaskDocumentation";

const dagId = getMetaValue("dag_id")!;

interface Props {
  taskId: string;
  runId: DagRun["runId"];
  mapIndex: GridTaskInstance["mapIndex"];
}

const TaskInstance = ({ taskId, runId, mapIndex }: Props) => {
  const taskInstanceRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(taskInstanceRef);
  const isMapIndexDefined = !(mapIndex === undefined);
  const {
    data: { dagRuns, groups },
  } = useGridData();

  const group = getTask({ taskId, task: groups });
  const run = dagRuns.find((r) => r.runId === runId);

  const children = group?.children;
  const isMapped = group?.isMapped;

  const isMappedTaskSummary = !!isMapped && !isMapIndexDefined && taskId;
  const isGroup = !!children;
  const isGroupOrMappedTaskSummary = isGroup || isMappedTaskSummary;

  const { data: taskInstance } = useTaskInstance({
    dagId,
    dagRunId: runId,
    taskId,
    mapIndex,
    options: {
      enabled: (!isGroup && !isMapped) || isMapIndexDefined,
    },
  });

  const showTaskSchedulingDependencies =
    !isGroupOrMappedTaskSummary &&
    (!taskInstance?.state || taskInstance?.state === "scheduled");

  const gridInstance = group?.instances.find((ti) => ti.runId === runId);

  return (
    <Box
      py="4px"
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={taskInstanceRef}
      overflowY="auto"
    >
      {!isGroup && run?.executionDate && (
        <TaskNav
          taskId={taskId}
          isMapped={isMapped}
          mapIndex={mapIndex}
          executionDate={run?.executionDate}
        />
      )}
      {!isGroupOrMappedTaskSummary && <TaskDocumentation taskId={taskId} />}
      {!isGroupOrMappedTaskSummary && (
        <NotesAccordion
          dagId={dagId}
          runId={runId}
          taskId={taskId}
          mapIndex={mapIndex}
          initialValue={gridInstance?.note || taskInstance?.note}
          key={dagId + runId + taskId + mapIndex}
          isAbandonedTask={!!taskId && !group}
        />
      )}
      {!!group?.extraLinks?.length &&
        !isGroupOrMappedTaskSummary &&
        run?.executionDate && (
          <ExtraLinks
            taskId={taskId}
            dagId={dagId}
            mapIndex={isMapped && isMapIndexDefined ? mapIndex : undefined}
            executionDate={run.executionDate}
            extraLinks={group.extraLinks}
            tryNumber={taskInstance?.tryNumber || gridInstance?.tryNumber || 1}
          />
        )}
      {group?.hasOutletDatasets && (
        <AssetUpdateEvents taskId={taskId} runId={runId} />
      )}
      <TriggererInfo taskInstance={taskInstance} />
      {showTaskSchedulingDependencies && (
        <TaskFailedDependency
          dagId={dagId}
          runId={runId}
          taskId={taskId}
          mapIndex={isMapped && isMapIndexDefined ? mapIndex : undefined}
        />
      )}
      <Details
        gridInstance={gridInstance}
        taskInstance={taskInstance}
        group={group}
      />
    </Box>
  );
};

export default TaskInstance;
