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
import { Box, Text } from '@chakra-ui/react';

import { formatDateTime } from '../datetime_utils';
import { getDuration, formatDuration } from './utils';

const InstanceTooltip = ({
  group,
  instance: {
    duration, operator, startDate, endDate, state, taskId, runId,
  },
}) => {
  const isGroup = !!group.children;

  const groupSummary = [];

  if (isGroup) {
    const numMap = new Map([
      ['success', 0],
      ['failed', 0],
      ['upstream_failed', 0],
      ['up_for_retry', 0],
      ['up_for_reschedule', 0],
      ['running', 0],
      ['deferred', 0],
      ['queued', 0],
      ['scheduled', 0],
      ['skipped', 0],
      ['no_status', 0],
    ]);
    group.children.forEach((child) => {
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

  const taskIdTitle = isGroup ? 'Task Group Id: ' : 'Task Id: ';

  return (
    <Box fontSize={12}>
      <br />
      <Text>
        <Text as="strong">Status:</Text>
        {' '}
        {state || 'no status'}
      </Text>
      {isGroup && (
        <>
          <br />
          <Text as="strong">Group Summary</Text>
          {groupSummary}
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
      <br />
    </Box>
  );
};

export default InstanceTooltip;
