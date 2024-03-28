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

import React, { useCallback, useEffect } from "react";
import { Flex, Box } from "@chakra-ui/react";
import { useSearchParams } from "react-router-dom";

import useSelection from "src/dag/useSelection";
import { getTask, getMetaValue } from "src/utils";
import { useGridData, useTaskInstance } from "src/api";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

import TaskInstanceContent from "./taskInstance";
import DagRunContent from "./dagRun";
import DagContent from "./dag/Dag";
import Graph from "./graph";
import Gantt from "./gantt";
import DagCode from "./dagCode";
import MappedInstances from "./taskInstance/MappedInstances";
import Logs from "./taskInstance/Logs";
import XcomCollection from "./taskInstance/Xcom";
import TaskDetails from "./task";
import AuditLog from "./AuditLog";
import RunDuration from "./dag/RunDuration";
import Calendar from "./dag/Calendar";
import DagButtons from "./DagButtons";
import RunButtons from "./RunButtons";
import TIButtons from "./TIButtons";

const dagId = getMetaValue("dag_id")!;

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
  gridScrollRef: React.RefObject<HTMLDivElement>;
  ganttScrollRef: React.RefObject<HTMLDivElement>;
}

export const TAB_PARAM = "tab";

const Details = ({
  openGroupIds,
  onToggleGroups,
  hoveredTaskState,
  gridScrollRef,
  ganttScrollRef,
}: Props) => {
  const {
    selected: { runId, taskId, mapIndex },
    onSelect,
  } = useSelection();

  const {
    data: { dagRuns, groups },
  } = useGridData();
  const group = getTask({ taskId, task: groups });
  const children = group?.children;
  const isMapped = group?.isMapped;
  const isGroup = !!children;

  const isMappedTaskSummary = !!(
    taskId &&
    runId &&
    !isGroup &&
    isMapped &&
    mapIndex === undefined
  );

  const isTaskInstance = !!(
    taskId &&
    runId &&
    !isGroup &&
    !isMappedTaskSummary
  );

  const isAbandonedTask = !!taskId && !group;

  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get(TAB_PARAM) || "graph";

  const onChangeTab = useCallback(
    (newTab?: string) => {
      const params = new URLSearchParamsWrapper(searchParams);

      if (newTab) params.set(TAB_PARAM, newTab);
      else params.delete(TAB_PARAM);
      setSearchParams(params);
    },
    [setSearchParams, searchParams]
  );

  useEffect(() => {
    // Redirect invalid tabs when run/task or tab selections change
    const taskInstanceTabs = ["logs", "mapped_tasks", "xcom"];
    const taskTabs = [...taskInstanceTabs, "task_details", "task_duration"];
    const runTabs = ["gantt", "run_details"];
    if (!runId && runTabs.some((t) => t === tab)) onChangeTab("graph");

    if (!taskId && taskTabs.some((t) => t === tab)) onChangeTab("graph");

    if (isGroup && taskInstanceTabs.some((t) => t === tab))
      onChangeTab("task_details");
    if (!isMapped && !isMappedTaskSummary && tab === "mapped_tasks")
      onChangeTab("logs");

    if (isMappedTaskSummary && tab === "logs") onChangeTab("mapped_tasks");
  }, [runId, taskId, isGroup, isMappedTaskSummary, onChangeTab, tab, isMapped]);

  const run = dagRuns.find((r) => r.runId === runId);
  const { data: mappedTaskInstance } = useTaskInstance({
    dagId,
    dagRunId: runId || "",
    taskId: taskId || "",
    mapIndex,
    enabled: mapIndex !== undefined,
  });

  const instance =
    mapIndex !== undefined && mapIndex > -1
      ? mappedTaskInstance
      : group?.instances.find((ti) => ti.runId === runId);

  return (
    <Flex flexDirection="column" height="100%" position="relative">
      <DagButtons dagId={dagId} tab={tab} onChangeTab={onChangeTab} />
      {runId && (
        <RunButtons
          runId={runId}
          tab={tab}
          onChangeTab={onChangeTab}
          run={run}
        />
      )}
      {taskId && (
        <TIButtons
          runId={runId}
          taskId={taskId}
          renderedMapIndex={mappedTaskInstance?.renderedMapIndex || mapIndex}
          tab={tab}
          onChangeTab={onChangeTab}
          isTaskInstance={isTaskInstance}
          isMappedTaskSummary={isMappedTaskSummary}
          run={run}
          isGroup={isGroup}
          isAbandonedTask={isAbandonedTask}
          mapIndex={mapIndex}
          isMapped={isMapped}
          state={
            !instance?.state || instance?.state === "none"
              ? undefined
              : instance.state
          }
        />
      )}
      <Box height="100%">
        {tab === "dag_details" && <DagContent />}
        {tab === "graph" && (
          <Graph
            openGroupIds={openGroupIds}
            onToggleGroups={onToggleGroups}
            hoveredTaskState={hoveredTaskState}
          />
        )}
        {tab === "code" && <DagCode />}
        {tab === "audit_log" && <AuditLog />}
        {tab === "run_duration" && <RunDuration />}
        {tab === "calendar" && <Calendar />}
        {tab === "gantt" && (
          <Gantt
            openGroupIds={openGroupIds}
            gridScrollRef={gridScrollRef}
            ganttScrollRef={ganttScrollRef}
          />
        )}
        {tab === "logs" && isTaskInstance && run && (
          <Logs
            dagId={dagId}
            dagRunId={runId}
            taskId={taskId}
            mapIndex={mapIndex}
            executionDate={run?.executionDate}
            tryNumber={instance?.tryNumber}
            state={
              !instance?.state || instance?.state === "none"
                ? undefined
                : instance.state
            }
          />
        )}
        {tab === "xcom" && isTaskInstance && (
          <XcomCollection
            dagId={dagId}
            dagRunId={runId}
            taskId={taskId}
            mapIndex={mapIndex}
            tryNumber={instance?.tryNumber}
          />
        )}
        {tab === "mapped_tasks" && runId && taskId && (
          <MappedInstances
            dagId={dagId}
            runId={runId}
            taskId={taskId}
            onRowClicked={(row) => {
              onChangeTab("logs");
              onSelect({ runId, taskId, mapIndex: row.values.mapIndex });
            }}
          />
        )}
        {tab === "run_details" && runId && <DagRunContent runId={runId} />}
        {tab === "task_details" && runId && taskId && (
          <TaskInstanceContent
            runId={runId}
            taskId={taskId}
            mapIndex={mapIndex}
          />
        )}
        {tab === "task_duration" && <TaskDetails />}
      </Box>
    </Flex>
  );
};

export default Details;
