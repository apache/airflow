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

import { useTreeData } from '../../../api';

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
  const { data: { groups = {} } } = useTreeData();
  const task = getTask({ taskId, runId, task: groups });
  if (!task) return null;

  const isGroup = !!task.children;

  const instance = task.instances.find((ti) => ti.runId === runId);

  const {
    dagId,
    executionDate,
    tryNumber,
  } = instance;

  return (
    <Box fontSize="12px" py="4px">
      {!isGroup && (
        <TaskNav instance={instance} isMapped={task.isMapped} />
      )}
      {!isGroup && (
        <>
          <VStack justifyContent="center" divider={<StackDivider my={3} />} my={3}>
            <RunAction runId={runId} taskId={task.id} dagId={dagId} />
            <ClearAction
              runId={runId}
              taskId={task.id}
              dagId={dagId}
              executionDate={executionDate}
            />
            <MarkFailedAction runId={runId} taskId={task.id} dagId={dagId} />
            <MarkSuccessAction runId={runId} taskId={task.id} dagId={dagId} />
          </VStack>
          <Divider my={2} />
        </>
      )}
      {!task.isMapped && (
        <Logs
          dagId={dagId}
          taskId={taskId}
          executionDate={executionDate}
          tryNumber={tryNumber}
        />
      )}
      <Details instance={instance} task={task} />
      <ExtraLinks
        taskId={taskId}
        dagId={dagId}
        executionDate={executionDate}
        extraLinks={task.extraLinks}
      />
    </Box>
  );
};

export default TaskInstance;
