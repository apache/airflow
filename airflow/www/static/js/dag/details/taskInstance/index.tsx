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
  Text,
  Tabs,
  TabList,
  Tab,
  TabPanels,
  TabPanel,
} from '@chakra-ui/react';

import { useGridData, useTaskInstance } from 'src/api';
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

  const group = getTask({ taskId, task: groups });
  const run = dagRuns.find((r) => r.runId === runId);

  const children = group?.children;
  const isMapped = group?.isMapped;
  const operator = group?.operator;

  const isMappedTaskSummary = !!isMapped && !isMapIndexDefined && taskId;
  const isGroup = !!children;
  const isGroupOrMappedTaskSummary = (isGroup || isMappedTaskSummary);

  const { data: mappedTaskInstance } = useTaskInstance({
    dagId, dagRunId: runId, taskId, mapIndex, enabled: isMapIndexDefined,
  });

  const instance = isMapIndexDefined
    ? mappedTaskInstance
    : group?.instances.find((ti) => ti.runId === runId);

  const handleTabsChange = (index: number) => {
    localStorage.setItem(detailsPanelActiveTabIndex, index.toString());
    setPreferedTabIndex(index);
  };

  if (!group || !run || !instance) return null;

  let isPreferedTabDisplayed = false;

  switch (preferedTabIndex) {
    case 0:
      isPreferedTabDisplayed = true;
      break;
    case 1:
      isPreferedTabDisplayed = !isGroup;
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
          {isMappedTaskSummary && (
            <Tab>
              <Text as="strong">Mapped Tasks</Text>
            </Tab>
          )}
          {!isGroupOrMappedTaskSummary && (
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
              <Details instance={instance} group={group} />
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
          {!isGroupOrMappedTaskSummary && (
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
          {
            isMappedTaskSummary && (
            <TabPanel>
              <MappedInstances
                dagId={dagId}
                runId={runId}
                taskId={taskId}
                onRowClicked={(row) => onSelect({ runId, taskId, mapIndex: row.values.mapIndex })}
              />
            </TabPanel>
            )
          }
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default TaskInstance;
