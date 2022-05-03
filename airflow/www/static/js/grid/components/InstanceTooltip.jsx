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
import { Box, Text } from '@chakra-ui/react';

import { finalStatesMap } from '../../utils';
import { formatDuration, getDuration } from '../../datetime_utils';
import Time from './Time';

const InstanceTooltip = ({
  group,
  instance: {
    startDate, endDate, state, runId, mappedStates,
  },
}) => {
  const isGroup = !!group.children;
  const { isMapped } = group;
  const summary = [];

  const numMap = finalStatesMap();
  if (isGroup) {
    group.children.forEach((child) => {
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
        <Text key={val} ml="10px">
          {val}
          {': '}
          {key}
        </Text>,
      );
    }
  });

  return (
    <Box py="2px">
      {group.tooltip && (
        <Text>{group.tooltip}</Text>
      )}
      {isMapped && !!mappedStates.length && (
        <Text>
          {mappedStates.length}
          {' '}
          mapped task
          {mappedStates.length > 1 && 's'}
        </Text>
      )}
      <Text>
        {(isGroup || isMapped) ? 'Overall ' : ''}
        Status:
        {' '}
        {state || 'no status'}
      </Text>
      {(isGroup || isMapped) && summary}
      <Text>
        Started:
        {' '}
        <Time dateTime={startDate} />
      </Text>
      <Text>
        Duration:
        {' '}
        {formatDuration(getDuration(startDate, endDate))}
      </Text>
    </Box>
  );
};

export default InstanceTooltip;
