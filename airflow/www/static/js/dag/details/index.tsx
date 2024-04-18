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
import {
  Flex,
  Divider,
  Tabs,
  TabList,
  TabPanel,
  TabPanels,
  Tab,
  Text,
  Button,
} from "@chakra-ui/react";
import { useSearchParams } from "react-router-dom";

import useSelection from "src/dag/useSelection";
import { getTask, getMetaValue } from "src/utils";
import { useGridData, useTaskInstance } from "src/api";
import {
  MdDetails,
  MdAccountTree,
  MdReorder,
  MdCode,
  MdOutlineViewTimeline,
  MdSyncAlt,
  MdHourglassBottom,
  MdPlagiarism,
  MdEvent,
} from "react-icons/md";
import { BiBracket } from "react-icons/bi";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

import Header from "./Header";
import TaskInstanceContent from "./taskInstance";
import DagRunContent from "./dagRun";
import DagContent from "./dag/Dag";
import Graph from "./graph";
import Gantt from "./gantt";
import DagCode from "./dagCode";
import MappedInstances from "./taskInstance/MappedInstances";
import Logs from "./taskInstance/Logs";
import BackToTaskSummary from "./taskInstance/BackToTaskSummary";
import FilterTasks from "./FilterTasks";
import ClearRun from "./dagRun/ClearRun";
import MarkRunAs from "./dagRun/MarkRunAs";
import ClearInstance from "./taskInstance/taskActions/ClearInstance";
import MarkInstanceAs from "./taskInstance/taskActions/MarkInstanceAs";
import XcomCollection from "./taskInstance/Xcom";
import TaskDetails from "./task";
import AuditLog from "./AuditLog";
import RunDuration from "./dag/RunDuration";
import Calendar from "./dag/Calendar";

const dagId = getMetaValue("dag_id")!;

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
  gridScrollRef: React.RefObject<HTMLDivElement>;
  ganttScrollRef: React.RefObject<HTMLDivElement>;
  isFullScreen?: boolean;
  toggleFullScreen?: () => void;
}

const tabToIndex = (tab?: string) => {
  switch (tab) {
    case "graph":
      return 1;
    case "gantt":
      return 2;
    case "code":
      return 3;
    case "audit_log":
      return 4;
    case "logs":
    case "mapped_tasks":
    case "run_duration":
      return 5;
    case "xcom":
    case "calendar":
      return 6;
    case "details":
    default:
      return 0;
  }
};

const indexToTab = (
  index: number,
  runId: string | null,
  taskId: string | null,
  isGroup: boolean,
  isMappedTaskSummary: boolean
) => {
  const isTaskInstance = !!(
    taskId &&
    runId &&
    !isGroup &&
    !isMappedTaskSummary
  );
  switch (index) {
    case 0:
      return "details";
    case 1:
      return "graph";
    case 2:
      return "gantt";
    case 3:
      return "code";
    case 4:
      return "audit_log";
    case 5:
      if (isMappedTaskSummary) return "mapped_tasks";
      if (isTaskInstance) return "logs";
      if (!runId && !taskId) return "run_duration";
      return undefined;
    case 6:
      if (!runId && !taskId) return "calendar";
      if (isTaskInstance) return "xcom";
      return undefined;
    default:
      return undefined;
  }
};

export const TAB_PARAM = "tab";

const Details = ({
  openGroupIds,
  onToggleGroups,
  hoveredTaskState,
  gridScrollRef,
  ganttScrollRef,
  isFullScreen,
  toggleFullScreen,
}: Props) => {
  const {
    selected: { runId, taskId, mapIndex },
    onSelect,
  } = useSelection();
  const isDag = !runId && !taskId;
  const isDagRun = runId && !taskId;

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

  const showTaskDetails = !!taskId && !runId;

  const isAbandonedTask = !!taskId && !group;

  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get(TAB_PARAM) || undefined;
  const tabIndex = tabToIndex(tab);

  const onChangeTab = useCallback(
    (index: number) => {
      const params = new URLSearchParamsWrapper(searchParams);
      const newTab = indexToTab(
        index,
        runId,
        taskId,
        isGroup,
        isMappedTaskSummary
      );
      if (newTab) params.set(TAB_PARAM, newTab);
      else params.delete(TAB_PARAM);
      setSearchParams(params);
    },
    [setSearchParams, searchParams, runId, taskId, isGroup, isMappedTaskSummary]
  );

  useEffect(() => {
    // Change to graph or task duration tab if the tab is no longer defined
    if (
      indexToTab(tabIndex, runId, taskId, isGroup, isMappedTaskSummary) ===
      undefined
    )
      onChangeTab(showTaskDetails ? 0 : 1);
  }, [
    tabIndex,
    runId,
    taskId,
    isGroup,
    isMappedTaskSummary,
    showTaskDetails,
    onChangeTab,
  ]);

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
    <Flex flexDirection="column" height="100%">
      <Flex
        alignItems="center"
        justifyContent="space-between"
        flexWrap="wrap"
        ml={6}
      >
        <Header
          mapIndex={
            mappedTaskInstance?.renderedMapIndex || mappedTaskInstance?.mapIndex
          }
        />
        <Flex flexWrap="wrap">
          {runId && !taskId && (
            <>
              <ClearRun runId={runId} mr={2} />
              <MarkRunAs runId={runId} state={run?.state} />
            </>
          )}
          {runId && taskId && !isAbandonedTask && (
            <>
              <ClearInstance
                taskId={taskId}
                runId={runId}
                executionDate={run?.executionDate || ""}
                isGroup={isGroup}
                isMapped={isMapped}
                mapIndex={mapIndex}
                mt={2}
                mr={2}
              />
              <MarkInstanceAs
                taskId={taskId}
                runId={runId}
                state={
                  !instance?.state || instance?.state === "none"
                    ? undefined
                    : instance.state
                }
                isGroup={isGroup}
                isMapped={isMapped}
                mapIndex={mapIndex}
                mt={2}
                mr={2}
              />
            </>
          )}
          <FilterTasks taskId={taskId} />
        </Flex>
      </Flex>
      <Divider my={2} />
      <Tabs
        size="lg"
        isLazy
        height="100%"
        index={tabIndex}
        onChange={onChangeTab}
      >
        <TabList>
          <Tab>
            <MdDetails size={16} />
            <Text as="strong" ml={1}>
              Details
            </Text>
          </Tab>
          <Tab>
            <MdAccountTree size={16} />
            <Text as="strong" ml={1}>
              Graph
            </Text>
          </Tab>
          <Tab>
            <MdOutlineViewTimeline size={16} />
            <Text as="strong" ml={1}>
              Gantt
            </Text>
          </Tab>
          <Tab>
            <MdCode size={16} />
            <Text as="strong" ml={1}>
              Code
            </Text>
          </Tab>
          <Tab>
            <MdPlagiarism size={16} />
            <Text as="strong" ml={1}>
              Audit Log
            </Text>
          </Tab>
          {isDag && (
            <Tab>
              <MdHourglassBottom size={16} />
              <Text as="strong" ml={1}>
                Run Duration
              </Text>
            </Tab>
          )}
          {isDag && (
            <Tab>
              <MdEvent size={16} />
              <Text as="strong" ml={1}>
                Calendar
              </Text>
            </Tab>
          )}
          {isTaskInstance && (
            <Tab>
              <MdReorder size={16} />
              <Text as="strong" ml={1}>
                Logs
              </Text>
            </Tab>
          )}
          {isMappedTaskSummary && (
            <Tab>
              <BiBracket size={16} />
              <Text as="strong" ml={1}>
                Mapped Tasks
              </Text>
            </Tab>
          )}
          {isTaskInstance && (
            <Tab>
              <MdSyncAlt size={16} />
              <Text as="strong" ml={1}>
                XCom
              </Text>
            </Tab>
          )}
          {/* Match the styling of a tab but its actually a button */}
          {!!taskId && !!runId && (
            <Button
              variant="unstyled"
              display="flex"
              alignItems="center"
              fontSize="lg"
              py={3}
              // need to split pl and pr instead of px
              pl={4}
              pr={4}
              mt="4px"
              onClick={() => {
                onChangeTab(0);
                onSelect({ taskId });
              }}
              isDisabled={isAbandonedTask}
            >
              <MdHourglassBottom size={16} />
              <Text as="strong" ml={1}>
                Task Duration
              </Text>
            </Button>
          )}
        </TabList>
        <TabPanels height="100%">
          <TabPanel height="100%">
            {isDag && <DagContent />}
            {isDagRun && <DagRunContent runId={runId} />}
            {!!runId && !!taskId && (
              <>
                <BackToTaskSummary
                  isMapIndexDefined={mapIndex !== undefined && mapIndex > -1}
                  onClick={() => onSelect({ runId, taskId })}
                />
                <TaskInstanceContent
                  runId={runId}
                  taskId={taskId}
                  mapIndex={mapIndex}
                />
              </>
            )}
            {showTaskDetails && <TaskDetails />}
          </TabPanel>
          <TabPanel p={0} height="100%">
            <Graph
              openGroupIds={openGroupIds}
              onToggleGroups={onToggleGroups}
              hoveredTaskState={hoveredTaskState}
              isFullScreen={isFullScreen}
              toggleFullScreen={toggleFullScreen}
            />
          </TabPanel>
          <TabPanel p={0} height="100%">
            <Gantt
              openGroupIds={openGroupIds}
              gridScrollRef={gridScrollRef}
              ganttScrollRef={ganttScrollRef}
            />
          </TabPanel>
          <TabPanel height="100%">
            <DagCode />
          </TabPanel>
          <TabPanel height="100%">
            <AuditLog
              taskId={isGroup || !taskId ? undefined : taskId}
              run={run}
            />
          </TabPanel>
          {isDag && (
            <TabPanel height="100%">
              <RunDuration />
            </TabPanel>
          )}
          {isDag && (
            <TabPanel height="100%" width="100%">
              <Calendar />
            </TabPanel>
          )}
          {isTaskInstance && run && (
            <TabPanel
              pt={mapIndex !== undefined ? "0px" : undefined}
              height="100%"
            >
              <BackToTaskSummary
                isMapIndexDefined={mapIndex !== undefined}
                onClick={() => onSelect({ runId, taskId })}
              />
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
                isFullScreen={isFullScreen}
                toggleFullScreen={toggleFullScreen}
              />
            </TabPanel>
          )}
          {isMappedTaskSummary && (
            <TabPanel height="100%">
              <MappedInstances
                dagId={dagId}
                runId={runId}
                taskId={taskId}
                onRowClicked={(row) =>
                  onSelect({ runId, taskId, mapIndex: row.values.mapIndex })
                }
              />
            </TabPanel>
          )}
          {isTaskInstance && (
            <TabPanel height="100%">
              <XcomCollection
                dagId={dagId}
                dagRunId={runId}
                taskId={taskId}
                mapIndex={mapIndex}
                tryNumber={instance?.tryNumber}
              />
            </TabPanel>
          )}
        </TabPanels>
      </Tabs>
    </Flex>
  );
};

export default Details;
