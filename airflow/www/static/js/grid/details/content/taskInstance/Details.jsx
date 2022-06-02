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
import { SimpleStatus } from '../../../components/StatusBox';
import Time from '../../../components/Time';
import { ClipboardText } from '../../../components/Clipboard';

const Details = ({ instance, group, operator }) => {
  const isGroup = !!group.children;
  const summary = [];

  const {
    taskId,
    runId,
    startDate,
    endDate,
    state,
    mappedStates,
  } = instance;

  const {
    isMapped,
    children,
    tooltip,
  } = group;

  const numMap = finalStatesMap();
  if (isGroup) {
    children.forEach((child) => {
      const taskInstance = child.instances.find((ti) => ti.runId === runId);
      if (taskInstance) {
        const stateKey = taskInstance.state == null ? 'no_status' : taskInstance.state;
        if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
      }
    });
  } else if (isMapped && mappedStates) {
    mappedStates.forEach((s) => {
      const stateKey = s || 'no_status';
      if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
    });
  }

  numMap.forEach((key, val) => {
    if (key > 0) {
      summary.push(
        // eslint-disable-next-line react/no-array-index-key
        <Flex key={val} ml="10px" alignItems="center">
          <SimpleStatus state={val} mx={2} />
          {val}
          {': '}
          {key}
        </Flex>,
      );
    }
  });

  const taskIdTitle = isGroup ? 'Task Group Id: ' : 'Task Id: ';
  const isStateFinal = ['success', 'failed', 'upstream_failed', 'skipped'].includes(state);
  const isOverall = (isMapped || isGroup) && 'Overall ';

  return (
    <Flex flexWrap="wrap" justifyContent="space-between">
      <Box>
        {tooltip && (
          <>
            <Text>{tooltip}</Text>
            <br />
          </>
        )}
        {mappedStates && mappedStates.length > 0 && (
        <Text>
          {mappedStates.length}
          {' '}
          {mappedStates.length === 1 ? 'Task ' : 'Tasks '}
          Mapped
        </Text>
        )}
        <Flex alignItems="center">
          <Text as="strong">
            {isOverall}
            Status:
          </Text>
          <SimpleStatus state={state} mx={2} />
          {state || 'no status'}
        </Flex>
        {summary.length > 0 && (
          summary
        )}
        <br />
        <Text>
          {taskIdTitle}
          <ClipboardText value={taskId} />
        </Text>
        <Text whiteSpace="nowrap">
          Run Id:
          {' '}
          <ClipboardText value={runId} />
        </Text>
        {operator && (
          <Text>
            Operator:
            {' '}
            {operator}
          </Text>
        )}
        <br />
        <Text>
          {isOverall}
          Duration:
          {' '}
          {formatDuration(getDuration(startDate, endDate))}
        </Text>
        {startDate && (
        <Text>
          Started:
          {' '}
          <Time dateTime={startDate} />
        </Text>
        )}
        {endDate && isStateFinal && (
        <Text>
          Ended:
          {' '}
          <Time dateTime={endDate} />
        </Text>
        )}
      </Box>
    </Flex>
  );
};

export default Details;
