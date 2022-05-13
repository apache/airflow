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
  Tr,
  Td,
  Text,
  Box,
  Flex,
} from '@chakra-ui/react';

import { useGridData } from '../api';
import DagRunBar from './Bar';
import { getDuration, formatDuration } from '../../datetime_utils';
import useSelection from '../utils/useSelection';

const DurationTick = ({ children, ...rest }) => (
  <Text fontSize="sm" color="gray.400" right={1} position="absolute" whiteSpace="nowrap" {...rest}>
    {children}
  </Text>
);

const DagRuns = () => {
  const { data: { dagRuns } } = useGridData();
  const { selected, onSelect } = useSelection();
  const durations = [];
  const runs = dagRuns.map((dagRun) => {
    const duration = getDuration(dagRun.startDate, dagRun.endDate);
    durations.push(duration);
    return {
      ...dagRun,
      duration,
    };
  });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(null, durations);
  const tickWidth = `${runs.length * 16}px`;

  return (
    <Tr
      borderBottomWidth={3}
      borderBottomColor="gray.200"
      position="relative"
    >
      <Td
        height="155px"
        p={0}
        position="sticky"
        left={0}
        backgroundColor="white"
        zIndex={2}
        borderBottom={0}
        width="100%"
      >
        {!!runs.length && (
        <>
          <DurationTick bottom="120px">Duration</DurationTick>
          <DurationTick bottom="96px">
            {formatDuration(max)}
          </DurationTick>
          <DurationTick bottom="46px">
            {formatDuration(max / 2)}
          </DurationTick>
          <DurationTick bottom={0}>
            00:00:00
          </DurationTick>
        </>
        )}
      </Td>
      <Td p={0} borderBottom={0}>
        <Box position="absolute" bottom="100px" borderBottomWidth={1} zIndex={0} opacity={0.7} width={tickWidth} />
        <Box position="absolute" bottom="50px" borderBottomWidth={1} zIndex={0} opacity={0.7} width={tickWidth} />
        <Box position="absolute" bottom="4px" borderBottomWidth={1} zIndex={0} opacity={0.7} width={tickWidth} />
      </Td>
      <Td p={0} align="right" verticalAlign="bottom" borderBottom={0} width={`${runs.length * 16}px`}>
        <Flex justifyContent="flex-end">
          {runs.map((run, i) => (
            <DagRunBar
              key={run.runId}
              run={run}
              max={max}
              index={i}
              totalRuns={runs.length}
              isSelected={run.runId === selected.runId}
              onSelect={onSelect}
            />
          ))}
        </Flex>
      </Td>
    </Tr>
  );
};

export default DagRuns;
