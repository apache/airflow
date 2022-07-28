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
  Table,
  Tbody,
  Tr,
  Td,
} from '@chakra-ui/react';

import { finalStatesMap } from 'src/utils';
import { getDuration, formatDuration } from 'src/datetime_utils';
import { SimpleStatus } from 'src/dag/StatusBox';
import Time from 'src/components/Time';
import { ClipboardText } from 'src/components/Clipboard';
import type { Task, TaskInstance, TaskState } from 'src/types';

interface Props {
  instance: TaskInstance;
  group: Task;
  operator: string;
}

const Details = ({ instance, group, operator }: Props) => {
  const isGroup = !!group.children;
  const summary: React.ReactNode[] = [];

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
    tooltip,
  } = group;

  const numMap = finalStatesMap();
  let numMapped = 0;
  if (isGroup) {
    group.children?.forEach((child) => {
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
        <Flex key={val} ml="10px" alignItems="center">
          <SimpleStatus state={val as TaskState} mx={2} />
          {val}
          {': '}
          {key}
        </Flex>,
      );
    }
  });

  const taskIdTitle = isGroup ? 'Task Group ID' : 'Task ID';
  const isStateFinal = state && ['success', 'failed', 'upstream_failed', 'skipped'].includes(state);
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
        {mappedStates && numMapped > 0 && (
          <Text>
            {numMapped}
            {' '}
            {numMapped === 1 ? 'Task ' : 'Tasks '}
            Mapped
          </Text>
        )}
        {summary.length > 0 && (
          summary
        )}
      </Box>
      <br />
      <br />
      <Table variant="striped">
        <Tbody>
          <Tr>
            <Td>
              {isOverall}
              Status
            </Td>
            <Td>
              <Flex>
                <SimpleStatus state={state} mx={2} />
                {state || 'no status'}
              </Flex>
            </Td>
          </Tr>
          <Tr>
            <Td>{taskIdTitle}</Td>
            <Td><ClipboardText value={taskId} /></Td>
          </Tr>
          <Tr>
            <Td>Run ID</Td>
            <Td><Text whiteSpace="nowrap"><ClipboardText value={runId} /></Text></Td>
          </Tr>
          {operator && (
            <Tr>
              <Td>Operator</Td>
              <Td>{operator}</Td>
            </Tr>
          )}
          <Tr>
            <Td>
              {isOverall}
              Duration
            </Td>
            <Td>{formatDuration(getDuration(startDate, endDate))}</Td>
          </Tr>
          {startDate && (
            <Tr>
              <Td>Started</Td>
              <Td><Time dateTime={startDate} /></Td>
            </Tr>
          )}
          {endDate && isStateFinal && (
            <Tr>
              <Td>Ended</Td>
              <Td><Time dateTime={endDate} /></Td>
            </Tr>
          )}
        </Tbody>
      </Table>
    </Flex>
  );
};

export default Details;
