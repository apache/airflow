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

import React from 'react';
import {
  Box,
  VStack,
  Divider,
  StackDivider,
} from '@chakra-ui/react';

import RunAction from './taskActions/Run';
import ClearAction from './taskActions/Clear';
import MarkFailedAction from './taskActions/MarkFailed';
import MarkSuccessAction from './taskActions/MarkSuccess';
import ExtraLinks from './ExtraLinks';
import Logs from './Logs';
import TaskNav from './Nav';
import Details from './Details';

import { useTreeData, useTasks } from '../../../api';
import MappedInstances from './MappedInstances';
import { getMetaValue } from '../../../../utils';

const dagId = getMetaValue('dag_id');

const getTask = ({ taskId, runId, task }) => {
  if (task.id === taskId) return task;
  if (task.children) {
    let foundTask;
    task.children.forEach((c) => {
      const childTask = getTask({ taskId, runId, task: c });
      if (childTask) foundTask = childTask;
    });
    return foundTask;
  }
  return null;
};

const TaskInstance = ({ taskId, runId }) => {
  const { data: { groups = {}, dagRuns = [] } } = useTreeData();
  const group = getTask({ taskId, runId, task: groups });
  const run = dagRuns.find((r) => r.runId === runId);
  const { executionDate } = run;
  const { data: { tasks } } = useTasks(dagId);
  if (!group) return null;
  const task = tasks.find((t) => t.taskId === taskId);
  const operator = task && task.classRef && task.classRef.className ? task.classRef.className : '';

  const isGroup = !!group.children;
  const { isMapped, extraLinks } = group;

  const instance = group.instances.find((ti) => ti.runId === runId);

  return (
    <Box py="4px">
      {!isGroup && (
        <TaskNav
          taskId={taskId}
          isMapped={isMapped}
          executionDate={executionDate}
          operator={operator}
        />
      )}
      {!isGroup && (
        <Box my={3}>
          <VStack justifyContent="center" divider={<StackDivider my={3} />}>
            <RunAction
              runId={runId}
              taskId={taskId}
              dagId={dagId}
            />
            <ClearAction
              runId={runId}
              taskId={taskId}
              dagId={dagId}
              executionDate={executionDate}
            />
            <MarkFailedAction
              runId={runId}
              taskId={taskId}
              dagId={dagId}
            />
            <MarkSuccessAction
              runId={runId}
              taskId={taskId}
              dagId={dagId}
            />
          </VStack>
          <Divider my={2} />
        </Box>
      )}
      {!isMapped && (
        <Logs
          dagId={dagId}
          taskId={taskId}
          executionDate={executionDate}
          tryNumber={instance.tryNumber}
        />
      )}
      <Details instance={instance} group={group} operator={operator} />
      <ExtraLinks
        taskId={taskId}
        dagId={dagId}
        executionDate={executionDate}
        extraLinks={extraLinks}
      />
      {isMapped && (
        <MappedInstances dagId={dagId} runId={runId} taskId={taskId} />
      )}
    </Box>
  );
};

export default TaskInstance;
