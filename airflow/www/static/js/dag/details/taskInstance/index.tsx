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
import type { DagRun, TaskInstance as TaskInstanceType } from "src/types";
import NotesAccordion from "src/dag/details/NotesAccordion";

import TaskNav from "./Nav";
import ExtraLinks from "./ExtraLinks";
import Details from "./Details";

const dagId = getMetaValue("dag_id")!;

interface Props {
  taskId: string;
  runId: DagRun["runId"];
  mapIndex: TaskInstanceType["mapIndex"];
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
  const operator = group?.operator;

  const isMappedTaskSummary = !!isMapped && !isMapIndexDefined && taskId;
  const isGroup = !!children;
  const isGroupOrMappedTaskSummary = isGroup || isMappedTaskSummary;

  const { data: mappedTaskInstance } = useTaskInstance({
    dagId,
    dagRunId: runId,
    taskId,
    mapIndex,
    enabled: isMapIndexDefined,
  });

  const instance = isMapIndexDefined
    ? mappedTaskInstance
    : group?.instances.find((ti) => ti.runId === runId);

  if (!group || !run || !instance) return null;

  const { executionDate } = run;

  return (
    <Box
      py="4px"
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={taskInstanceRef}
      overflowY="auto"
    >
      {!isGroup && (
        <TaskNav
          taskId={taskId}
          isMapped={isMapped}
          mapIndex={mapIndex}
          executionDate={executionDate}
          operator={operator}
        />
      )}
      {!isGroupOrMappedTaskSummary && (
        <NotesAccordion
          dagId={dagId}
          runId={runId}
          taskId={taskId}
          mapIndex={instance.mapIndex}
          initialValue={instance.note}
          key={dagId + runId + taskId + instance.mapIndex}
        />
      )}
      {isMapped && group.extraLinks && isMapIndexDefined && (
        <ExtraLinks
          taskId={taskId}
          dagId={dagId}
          mapIndex={mapIndex}
          executionDate={executionDate}
          extraLinks={group?.extraLinks}
          tryNumber={instance.tryNumber}
        />
      )}
      {!isMapped && group.extraLinks && (
        <ExtraLinks
          taskId={taskId}
          dagId={dagId}
          executionDate={executionDate}
          extraLinks={group?.extraLinks}
          tryNumber={instance.tryNumber}
        />
      )}
      <Details instance={instance} group={group} dagId={dagId} />
    </Box>
  );
};

export default TaskInstance;
