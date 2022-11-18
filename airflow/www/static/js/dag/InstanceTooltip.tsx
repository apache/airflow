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

import { finalStatesMap } from 'src/utils';
import { formatDuration, getDuration } from 'src/datetime_utils';
import type { TaskInstance, Task } from 'src/types';
import Time from 'src/components/Time';

interface Props {
  group: Task;
  instance: TaskInstance;
}

const InstanceTooltip = ({
  group,
  instance: {
    startDate, endDate, state, runId, mappedStates, notes,
  },
}: Props) => {
  if (!group) return null;
  const isGroup = !!group.children;
  const summary: React.ReactNode[] = [];

  const isMapped = group?.isMapped;

  const numMap = finalStatesMap();
  let numMapped = 0;
  if (isGroup && group.children) {
    group.children.forEach((child) => {
      const taskInstance = child.instances.find((ti) => ti.runId === runId);
      if (taskInstance) {
        const stateKey = taskInstance.state == null ? 'no_status' : taskInstance.state;
        if (numMap.has(stateKey)) numMap.set(stateKey, (numMap.get(stateKey) || 0) + 1);
      }
    });
  } else if (isMapped && mappedStates) {
    Object.keys(mappedStates).forEach((stateKey) => {
      const num = mappedStates[stateKey];
      numMapped += num;
      numMap.set(stateKey || 'no_status', num);
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
      {isMapped && numMapped > 0 && (
        <Text>
          {numMapped}
          {' '}
          mapped task
          {numMapped > 1 && 's'}
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
      {notes && (
        <Text>Contains a note</Text>
      )}
    </Box>
  );
};

export default InstanceTooltip;
