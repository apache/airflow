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

import React, { useState } from 'react';
import {
  Box,
  VStack,
  Divider,
  StackDivider,
  Text,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
} from '@chakra-ui/react';

import { useGridData } from 'src/api';
import { getMetaValue, getTask } from 'src/utils';
import type { Task, DagRun } from 'src/types';

import RunAction from './taskActions/Run';
import ClearAction from './taskActions/Clear';
import MarkFailedAction from './taskActions/MarkFailed';
import MarkSuccessAction from './taskActions/MarkSuccess';
import ExtraLinks from './ExtraLinks';
import Logs from './Logs';
import TaskNav from './Nav';
import Details from './Details';
import MappedInstances from './MappedInstances';

const detailsPanelActiveTabIndex = 'detailsPanelActiveTabIndex';

const dagId = getMetaValue('dag_id')!;

interface Props {
  taskId: Task['id'];
  runId: DagRun['runId'];
}

const TaskInstance = ({ taskId, runId }: Props) => {
  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const { data: { dagRuns, groups } } = useGridData();

  const storageTabIndex = parseInt(localStorage.getItem(detailsPanelActiveTabIndex) || '0', 10);
  const [preferedTabIndex, setPreferedTabIndex] = useState(storageTabIndex);

  const group = getTask({ taskId, task: groups });
  const run = dagRuns.find((r) => r.runId === runId);

  if (!group || !run) return null;

  const { children, isMapped, operator } = group;

  const handleTabsChange = (index: number) => {
    localStorage.setItem(detailsPanelActiveTabIndex, index.toString());
    setPreferedTabIndex(index);
  };

  const isGroup = !!children;
  const isSimpleTask = !isMapped && !isGroup;

  let isPreferedTabDisplayed = false;

  switch (preferedTabIndex) {
    case 0:
      isPreferedTabDisplayed = true;
      break;
    case 1:
      isPreferedTabDisplayed = isSimpleTask;
      break;
    default:
      isPreferedTabDisplayed = false;
  }

  const selectedTabIndex = isPreferedTabDisplayed ? preferedTabIndex : 0;

  const { executionDate } = run;

  const instance = group.instances.find((ti) => ti.runId === runId);
  if (!instance) return null;

  let taskActionsTitle = 'Task Actions';
  if (isMapped) {
    taskActionsTitle += ` for ${selectedRows.length || 'all'} mapped task${selectedRows.length !== 1 ? 's' : ''}`;
  }

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
          { isSimpleTask && (
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
                <Box my={3}>
                  <Text as="strong">{taskActionsTitle}</Text>
                  <Divider my={2} />
                  <VStack justifyContent="center" divider={<StackDivider my={3} />}>
                    <RunAction
                      runId={runId}
                      taskId={taskId}
                      dagId={dagId}
                      mapIndexes={selectedRows}
                    />
                    <ClearAction
                      runId={runId}
                      taskId={taskId}
                      dagId={dagId}
                      executionDate={executionDate}
                      mapIndexes={selectedRows}
                    />
                    <MarkFailedAction
                      runId={runId}
                      taskId={taskId}
                      dagId={dagId}
                      mapIndexes={selectedRows}
                    />
                    <MarkSuccessAction
                      runId={runId}
                      taskId={taskId}
                      dagId={dagId}
                      mapIndexes={selectedRows}
                    />
                  </VStack>
                  <Divider my={2} />
                </Box>
              )}
              <Details instance={instance} group={group} />
              {!isMapped && (
                <ExtraLinks
                  taskId={taskId}
                  dagId={dagId || ''}
                  executionDate={executionDate}
                  extraLinks={group?.extraLinks || []}
                />
              )}
              {isMapped && taskId && (
                <MappedInstances
                  dagId={dagId}
                  runId={runId}
                  taskId={taskId}
                  selectRows={setSelectedRows}
                />
              )}
            </Box>
          </TabPanel>
          {/* Logs Tab */}
          { isSimpleTask && (
          <TabPanel>
            <Logs
              dagId={dagId}
              dagRunId={runId}
              taskId={taskId!}
              executionDate={executionDate}
              tryNumber={instance?.tryNumber}
            />
          </TabPanel>
          )}
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default TaskInstance;
