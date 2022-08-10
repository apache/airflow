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

/* global localStorage */

import React, { useCallback, useEffect, useState } from 'react';
import {
  Box,
  Text,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
} from '@chakra-ui/react';

import { useGridData } from 'src/api';
import { getMetaValue, getTask } from 'src/utils';
import type {
  Task, DagRun, TaskInstance as TaskInstanceType,
} from 'src/types';

import type { SelectionProps } from 'src/dag/useSelection';
import ExtraLinks from './ExtraLinks';
import Logs from './Logs';
import TaskNav from './Nav';
import Details from './Details';
import MappedInstances from './MappedInstances';
import TaskActions from './taskActions';
import BackToTaskSummary from './BackToTaskSummary';

const detailsPanelActiveTabIndex = 'detailsPanelActiveTabIndex';

const dagId = getMetaValue('dag_id')!;

interface Props {
  taskId: Task['id'];
  runId: DagRun['runId'];
  mapIndex: TaskInstanceType['mapIndex'];
  onSelect: (selectionProps: SelectionProps) => void;
}

const TaskInstance = ({
  taskId, runId, mapIndex, onSelect,
}: Props) => {
  const isMapIndexDefined = !(mapIndex === undefined);
  const actionsMapIndexes = isMapIndexDefined ? [mapIndex] : [];
  const { data: { dagRuns, groups } } = useGridData();

  const storageTabIndex = parseInt(localStorage.getItem(detailsPanelActiveTabIndex) || '0', 10);
  const [preferedTabIndex, setPreferedTabIndex] = useState(storageTabIndex);

  const [instance, setInstance] = useState<TaskInstanceType>();

  const group = getTask({ taskId, task: groups });
  const run = dagRuns.find((r) => r.runId === runId);

  useEffect(() => {
    if (!isMapIndexDefined) {
      setInstance(group?.instances.find((ti) => ti.runId === runId));
    }
  }, [group, runId, isMapIndexDefined]);

  const setInstanceForMappedIndex = useCallback((
    rowMapIndex: number | undefined,
    mappedTaskInstances: TaskInstanceType[],
  ) => {
    const taskInstance = mappedTaskInstances.find((ti) => ti.mapIndex === rowMapIndex);
    if (taskInstance) {
      setInstance(taskInstance);
    }
  }, [setInstance]);

  if (!group || !run) return null;

  const { children, isMapped, operator } = group;

  const handleTabsChange = (index: number) => {
    localStorage.setItem(detailsPanelActiveTabIndex, index.toString());
    setPreferedTabIndex(index);
  };

  const isMappedTaskSummary = !!isMapped && !isMapIndexDefined;
  const isGroup = !!children;
  const showLogs = !(isGroup || isMappedTaskSummary);

  let isPreferedTabDisplayed = false;

  switch (preferedTabIndex) {
    case 0:
      isPreferedTabDisplayed = true;
      break;
    case 1:
      isPreferedTabDisplayed = showLogs || isMappedTaskSummary;
      break;
    default:
      isPreferedTabDisplayed = false;
  }

  const selectedTabIndex = isPreferedTabDisplayed ? preferedTabIndex : 0;

  const { executionDate } = run;

  let taskActionsTitle = 'Task Actions';
  if (isMapped) {
    taskActionsTitle += ` for ${actionsMapIndexes.length || 'all'} mapped task${actionsMapIndexes.length !== 1 ? 's' : ''}`;
  }

  const onRowClicked = (rowMapIndex: number, mappedTaskInstances: TaskInstanceType[]) => {
    setInstanceForMappedIndex(rowMapIndex, mappedTaskInstances);
    onSelect({ runId, taskId, mapIndex: rowMapIndex });
  };

  return (
    <Box py="4px">
      {!isGroup && (
        <TaskNav
          taskId={taskId}
          runId={runId}
          isMapped={isMapped}
          mapIndex={mapIndex}
          executionDate={executionDate}
          operator={operator}
        />
      )}
      <Tabs size="lg" index={selectedTabIndex} onChange={handleTabsChange}>
        <TabList>
          <Tab>
            <Text as="strong">Details</Text>
          </Tab>
          { isMappedTaskSummary && (
            <Tab>
              <Text as="strong">Mapped Tasks</Text>
            </Tab>
          )}
          { showLogs && (
            <Tab>
              <Text as="strong">Logs</Text>
            </Tab>
          )}
        </TabList>

        <BackToTaskSummary
          isMapIndexDefined={isMapIndexDefined}
          onClick={() => onSelect({ runId, taskId })}
        />

        <TabPanels>

          {/* Details Tab */}
          <TabPanel pt={isMapIndexDefined ? '0px' : undefined}>
            <Box py="4px">
              {!isGroup && (
              <TaskActions
                title={taskActionsTitle}
                runId={runId}
                taskId={taskId}
                dagId={dagId}
                executionDate={executionDate}
                mapIndexes={actionsMapIndexes}
              />
              )}
              {instance && <Details instance={instance} group={group} />}
              {!isMapped && (
                <ExtraLinks
                  taskId={taskId}
                  dagId={dagId}
                  executionDate={executionDate}
                  extraLinks={group?.extraLinks || []}
                />
              )}
            </Box>
          </TabPanel>

          {/* Logs Tab */}
          { showLogs && (
          <TabPanel pt={isMapIndexDefined ? '0px' : undefined}>
            <Logs
              dagId={dagId}
              dagRunId={runId}
              taskId={taskId!}
              mapIndex={mapIndex}
              executionDate={executionDate}
              tryNumber={instance?.tryNumber}
            />
          </TabPanel>
          )}

          {/* Mapped Task Instances Tab */}
          <TabPanel pt={isMapIndexDefined ? '0px' : undefined}>
            {isMapped && taskId && (
            <MappedInstances
              dagId={dagId}
              runId={runId}
              taskId={taskId}
              onRowClicked={onRowClicked}
              mapIndex={mapIndex}
              onMappedInstancesFetch={
                    (taskInstances) => setInstanceForMappedIndex(mapIndex, taskInstances)
                  }
            />
            )}
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default TaskInstance;
