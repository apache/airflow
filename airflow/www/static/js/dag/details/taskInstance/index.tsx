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
  const selectedRows: number[] = isMapIndexDefined ? [mapIndex] : [];
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

  const isGroup = !!children;
  const showLogs = !isGroup && ((!isMapped) || (isMapped && isMapIndexDefined)) && !!instance;

  let isPreferedTabDisplayed = false;

  switch (preferedTabIndex) {
    case 0:
      isPreferedTabDisplayed = true;
      break;
    case 1:
      isPreferedTabDisplayed = showLogs;
      break;
    default:
      isPreferedTabDisplayed = false;
  }

  const selectedTabIndex = isPreferedTabDisplayed ? preferedTabIndex : 0;

  const { executionDate } = run;

  let taskActionsTitle = 'Task Actions';
  if (isMapped) {
    taskActionsTitle += ` for ${selectedRows.length || 'all'} mapped task${selectedRows.length !== 1 ? 's' : ''}`;
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
          executionDate={executionDate}
          operator={operator}
        />
      )}
      <Tabs size="lg" index={selectedTabIndex} onChange={handleTabsChange}>
        <TabList>
          <Tab>
            <Text as="strong">Details</Text>
          </Tab>
          { showLogs && (
            <Tab>
              <Text as="strong">Logs</Text>
            </Tab>
          )}
        </TabList>
        <TabPanels>
          {/* Details Tab */}
          <TabPanel>
            <Box py="4px">
              {!isGroup && (
              <TaskActions
                title={taskActionsTitle}
                runId={runId}
                taskId={taskId}
                dagId={dagId}
                executionDate={executionDate}
                mapIndexes={selectedRows}
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
            </Box>
          </TabPanel>
          {/* Logs Tab */}
          { showLogs && (
          <TabPanel>
            <Logs
              dagId={dagId}
              dagRunId={runId}
              taskId={taskId!}
              mapIndex={mapIndex}
              executionDate={executionDate}
              tryNumber={instance.tryNumber}
            />
          </TabPanel>
          )}
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default TaskInstance;
