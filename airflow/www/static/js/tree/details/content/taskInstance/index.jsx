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
  Text,
  Box,
  Button,
  Flex,
  Link,
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

import { finalStatesMap, getMetaValue } from '../../../../utils';
import { getDuration, formatDuration } from '../../../../datetime_utils';
import { SimpleStatus } from '../../../StatusBox';
import Time from '../../../Time';
import { useTreeData } from '../../../api';

const isK8sExecutor = getMetaValue('k8s_or_k8scelery_executor') === 'True';
const numRuns = getMetaValue('num_runs');
const baseDate = getMetaValue('base_date');
const taskInstancesUrl = getMetaValue('task_instances_url');
const renderedK8sUrl = getMetaValue('rendered_k8s_url');
const renderedTemplatesUrl = getMetaValue('rendered_templates_url');
const logUrl = getMetaValue('log_url');
const taskUrl = getMetaValue('task_url');
const gridUrl = getMetaValue('grid_url');

const LinkButton = ({ children, ...rest }) => (<Button as={Link} variant="ghost" colorScheme="blue" {...rest}>{children}</Button>);

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
  const groupSummary = [];
  const mapSummary = [];

  const instance = task.instances.find((ti) => ti.runId === runId);

  const {
    dagId,
    duration,
    operator,
    startDate,
    endDate,
    state,
    mappedStates,
    executionDate,
    tryNumber,
  } = instance;

  if (isGroup) {
    const numMap = finalStatesMap();
    task.children.forEach((child) => {
      const taskInstance = child.instances.find((ti) => ti.runId === runId);
      if (taskInstance) {
        const stateKey = taskInstance.state == null ? 'no_status' : taskInstance.state;
        if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
      }
    });
    numMap.forEach((key, val) => {
      if (key > 0) {
        groupSummary.push(
          // eslint-disable-next-line react/no-array-index-key
          <Text key={val} ml="10px">
            {val}
            {': '}
            {key}
          </Text>,
        );
      }
    });
  }

  if (task.isMapped && mappedStates) {
    const numMap = finalStatesMap();
    mappedStates.forEach((s) => {
      const stateKey = s || 'no_status';
      if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
    });
    numMap.forEach((key, val) => {
      if (key > 0) {
        mapSummary.push(
          // eslint-disable-next-line react/no-array-index-key
          <Text key={val} ml="10px">
            {val}
            {': '}
            {key}
          </Text>,
        );
      }
    });
  }

  const taskIdTitle = isGroup ? 'Task Group Id: ' : 'Task Id: ';

  const params = new URLSearchParams({
    task_id: task.id,
    execution_date: executionDate,
  }).toString();
  const detailsLink = `${taskUrl}&${params}`;
  const renderedLink = `${renderedTemplatesUrl}&${params}`;
  const logLink = `${logUrl}&${params}`;
  const k8sLink = `${renderedK8sUrl}&${params}`;
  const listParams = new URLSearchParams({
    _flt_3_dag_id: dagId,
    _flt_3_task_id: taskId,
    _oc_TaskInstanceModelView: executionDate,
  });
  const allInstancesLink = `${taskInstancesUrl}?${listParams}`;
  // TODO: base subdag zooming as its own attribute instead of via operator name

  const subDagParams = new URLSearchParams({
    execution_date: executionDate,
  }).toString();

  const filterParams = new URLSearchParams({
    base_date: baseDate,
    num_runs: numRuns,
    root: taskId,
  }).toString();

  const subDagLink = `${gridUrl.replace(dagId, `${dagId}.${taskId}`)}?${subDagParams}`;
  const isSubDag = operator === 'SubDagOperator';

  return (
    <Box fontSize="12px" py="4px">
      {!isGroup && (
        <>
          <Flex flexWrap="wrap">
            {!task.isMapped && (
              <>
                <LinkButton href={detailsLink}>Task Instance Details</LinkButton>
                <LinkButton href={renderedLink}>Rendered Template</LinkButton>
                {isK8sExecutor && (
                <LinkButton href={k8sLink}>K8s Pod Spec</LinkButton>
                )}
                {isSubDag && (
                <LinkButton href={subDagLink}>Zoom into SubDag</LinkButton>
                )}
                <LinkButton href={logLink}>Log</LinkButton>
              </>
            )}
            <LinkButton href={allInstancesLink}>All Instances</LinkButton>
            <LinkButton href={`?${filterParams}`}>Filter Upstream</LinkButton>
          </Flex>
          <Divider mt={3} />
        </>
      )}
      {!isGroup && !task.isMapped && (
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
      <Flex flexWrap="wrap" justifyContent="space-between">
        <Box>
          {task.tooltip && (
          <Text>{task.tooltip}</Text>
          )}
          <Flex alignItems="center">
            <Text as="strong">Status:</Text>
            <SimpleStatus state={state} mx={2} />
            {state || 'no status'}
          </Flex>
          {isGroup && (
          <>
            <br />
            <Text as="strong">Task Group Summary</Text>
            {groupSummary}
          </>
          )}
          {task.isMapped && (
          <>
            <br />
            <Text as="strong">
              {mappedStates.length}
              {' '}
              {mappedStates.length === 1 ? 'Task ' : 'Tasks '}
              Mapped
            </Text>
            {mapSummary}
          </>
          )}
          <br />
          <Text>
            {taskIdTitle}
            {taskId}
          </Text>
          <Text whiteSpace="nowrap">
            Run Id:
            {' '}
            {runId}
          </Text>
          {operator && (
          <Text>
            Operator:
            {' '}
            {operator}
          </Text>
          )}
          <Text>
            Duration:
            {' '}
            {formatDuration(duration || getDuration(startDate, endDate))}
          </Text>
        </Box>
        <Box>
          <Text>
            Started:
            {' '}
            <Time dateTime={startDate} />
          </Text>
          <Text>
            Ended:
            {' '}
            <Time dateTime={endDate} />
          </Text>
        </Box>
      </Flex>
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
