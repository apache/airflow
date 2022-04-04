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
  Flex,
} from '@chakra-ui/react';

import { finalStatesMap } from '../../../../utils';
import { getDuration, formatDuration } from '../../../../datetime_utils';
import { SimpleStatus } from '../../../StatusBox';
import Time from '../../../Time';

const Details = ({ instance, task }) => {
  const isGroup = !!task.children;
  const groupSummary = [];
  const mapSummary = [];

  const {
    taskId,
    runId,
    duration,
    operator,
    startDate,
    endDate,
    state,
    mappedStates,
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

  return (
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
  );
};

export default Details;
