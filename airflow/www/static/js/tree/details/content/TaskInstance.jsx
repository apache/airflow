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

/* global moment */

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

import { finalStatesMap, getMetaValue } from '../../../utils';
import { formatDateTime, getDuration, formatDuration } from '../../../datetime_utils';
import { SimpleStatus } from '../../StatusBox';

const isK8sExecutor = getMetaValue('k8s_or_k8scelery_executor') === 'True';
const numRuns = getMetaValue('num_runs');
const baseDate = getMetaValue('base_date');
const logsWithMetadataUrl = getMetaValue('logs_with_metadata_url');
const showExternalLogRedirect = getMetaValue('show_external_log_redirect') === 'True';
const externalLogUrl = getMetaValue('external_log_url');
const externalLogName = getMetaValue('external_log_name');

const LinkButton = ({ children, ...rest }) => (<Button as={Link} variant="ghost" colorScheme="blue" {...rest}>{children}</Button>);

const TaskInstance = ({
  instance: {
    dagId,
    duration,
    operator,
    startDate,
    endDate,
    state,
    taskId,
    runId,
    mappedStates,
    executionDate,
    tryNumber,
  },
  task,
}) => {
  const isGroup = !!task.children;
  const groupSummary = [];
  const mapSummary = [];

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
    dag_id: dagId,
    task_id: task.id,
    execution_date: executionDate,
  }).toString();
  const detailsLink = `/task?${params}`;
  const renderedLink = `/rendered-templates?${params}`;
  const logLink = `/log?${params}`;
  const k8sLink = `/rendered-k8s?${params}`;
  const listParams = new URLSearchParams({
    _flt_3_dag_id: dagId,
    _flt_3_task_id: taskId,
    _oc_TaskInstanceModelView: executionDate,
  });
  const allInstancesLink = `/taskinstance/list?${listParams}`;
  // TODO: base subdag zooming as its own attribute instead of via operator name

  const subDagParams = new URLSearchParams({
    execution_date: executionDate,
  }).toString();

  const filterParams = new URLSearchParams({
    base_date: baseDate,
    num_runs: numRuns,
    root: taskId,
  }).toString();

  const subDagLink = `/dags/${dagId}.${taskId}/grid?${subDagParams}`;
  const isSubDag = operator === 'SubDagOperator';

  const externalLogs = [];

  const logAttempts = [...Array(tryNumber + 1)].map((_, index) => {
    if (index === 0 && tryNumber < 2) return null;

    const isExternal = index !== 0 && showExternalLogRedirect;

    if (isExternal) {
      const fullExternalUrl = `${externalLogUrl}
      ?dag_id=${encodeURIComponent(dagId)}
      &task_id=${encodeURIComponent(taskId)}
      &execution_date=${encodeURIComponent(executionDate)}
      &try_number=${index}`;
      externalLogs.push(
        <LinkButton
          // eslint-disable-next-line react/no-array-index-key
          key={index}
          href={fullExternalUrl}
          target="_blank"
        >
          {index}
        </LinkButton>,
      );
    }

    const fullMetadataUrl = `${logsWithMetadataUrl}
    ?dag_id=${encodeURIComponent(dagId)}
    &task_id=${encodeURIComponent(taskId)}
    &execution_date=${encodeURIComponent(executionDate)}
    ${index > 0 && `&try_number=${index}`}
    &metadata=null&format=file`;

    return (
      <LinkButton
        // eslint-disable-next-line react/no-array-index-key
        key={index}
        href={fullMetadataUrl}
      >
        {index === 0 ? 'All' : index}
      </LinkButton>
    );
  });

  return (
    <Box fontSize="12px" py="4px">
      {!isGroup && !task.isMapped && (
        <>
          <Flex justifyContent="space-between" flexWrap="wrap">
            <LinkButton href={detailsLink}>More Details</LinkButton>
            <LinkButton href={renderedLink}>Rendered Template</LinkButton>
            {isK8sExecutor && (
            <LinkButton href={k8sLink}>K8s Pod Spec</LinkButton>
            )}
            {isSubDag && (
              <LinkButton href={subDagLink}>Zoom into SubDag</LinkButton>
            )}
            <LinkButton href={logLink}>Log</LinkButton>
            <LinkButton href={allInstancesLink}>All Instances</LinkButton>
            <LinkButton href={`?${filterParams}`}>Filter Upstream</LinkButton>
          </Flex>
          <Divider mt={3} />
        </>
      )}
      {tryNumber > 0 && (
        <>
          <Box>
            <Text>Download Log (by attempts):</Text>
            <Flex flexWrap="wrap">
              {logAttempts}
            </Flex>
          </Box>
          <Divider mt={3} />
        </>
      )}
      {externalLogName && externalLogs.length > 0 && (
        <>
          <Box>
            <Text>
              View Logs in
              {' '}
              {externalLogName}
              {' '}
              (by attempts):
            </Text>
            <Flex flexWrap="wrap">
              {externalLogs}
            </Flex>
          </Box>
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
      <br />
      <Text as="strong">UTC</Text>
      <Text>
        Started:
        {' '}
        {startDate && formatDateTime(moment.utc(startDate))}
      </Text>
      <Text>
        Ended:
        {' '}
        {endDate && formatDateTime(moment.utc(endDate))}
      </Text>
      <br />
      <Text as="strong">
        Local:
        {' '}
        {moment().format('Z')}
      </Text>
      <Text>
        Started:
        {' '}
        {startDate && formatDateTime(startDate)}
      </Text>
      <Text>
        Ended:
        {' '}
        {endDate && formatDateTime(endDate)}
      </Text>
    </Box>
  );
};

export default TaskInstance;
