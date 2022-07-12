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
  TextProps,
  TableCellProps,
  Flex,
  BoxProps,
} from '@chakra-ui/react';

import { useGridData } from 'grid/api';
import { getDuration, formatDuration } from 'app/datetime_utils';
import useSelection from 'grid/utils/useSelection';
import type { DagRun } from 'grid/types';

import DagRunBar from './Bar';

const DurationAxis = (props: BoxProps) => (
  <Box position="absolute" borderBottomWidth={1} zIndex={0} opacity={0.7} width="100%" {...props} />
);

const DurationTick = ({ children, ...rest }: TextProps) => (
  <Text fontSize="sm" color="gray.400" right={1} position="absolute" whiteSpace="nowrap" {...rest}>
    {children}
  </Text>
);

const Th = (props: TableCellProps) => (
  <Td position="sticky" top={0} zIndex={1} p={0} height="155px" bg="white" {...props} />
);

export interface RunWithDuration extends DagRun {
  duration: number;
}

const DagRuns = () => {
  const { data: { dagRuns } } = useGridData();
  const { selected, onSelect } = useSelection();
  const durations: number[] = [];
  const runs: RunWithDuration[] = dagRuns.map((dagRun) => {
    const duration = getDuration(dagRun.startDate, dagRun.endDate);
    durations.push(duration);
    return {
      ...dagRun,
      duration,
    };
  });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(null, durations);

  return (
    <Tr>
      <Th left={0} zIndex={2}>
        <Box borderBottomWidth={3} position="relative" height="100%" width="100%">
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
        </Box>
      </Th>
      <Th align="right" verticalAlign="bottom">
        <Flex justifyContent="flex-end" borderBottomWidth={3} position="relative">
          {runs.map((run: RunWithDuration, index) => (
            <DagRunBar
              key={run.runId}
              run={run}
              index={index}
              totalRuns={runs.length}
              max={max}
              isSelected={run.runId === selected.runId}
              onSelect={onSelect}
            />
          ))}
          <DurationAxis bottom="100px" />
          <DurationAxis bottom="50px" />
          <DurationAxis bottom="4px" />
        </Flex>
      </Th>
    </Tr>
  );
};

export default DagRuns;
