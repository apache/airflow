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
  Flex,
  Text,
  Box,
  Button,
  Link,
  Divider,
} from '@chakra-ui/react';

import { MdPlayArrow, MdOutlineSchedule, MdOutlineAccountTree } from 'react-icons/md';
import { RiArrowGoBackFill } from 'react-icons/ri';

import { useGridData } from 'grid/api';
import { appendSearchParams, getMetaValue } from 'app/utils';
import type { DagRun as DagRunType } from 'grid/types';
import { SimpleStatus } from 'grid/components/StatusBox';
import { ClipboardText } from 'grid/components/Clipboard';
import { formatDuration, getDuration } from 'app/datetime_utils';
import Time from 'grid/components/Time';

import MarkFailedRun from './MarkFailedRun';
import MarkSuccessRun from './MarkSuccessRun';
import QueueRun from './QueueRun';
import ClearRun from './ClearRun';

const dagId = getMetaValue('dag_id');
const graphUrl = getMetaValue('graph_url');
const dagRunDetailsUrl = getMetaValue('dagrun_details_url');

interface Props {
  runId: DagRunType['runId'];
}

const DagRun = ({ runId }: Props) => {
  const { data: { dagRuns } } = useGridData();
  const run = dagRuns.find((dr) => dr.runId === runId);
  if (!run) return null;
  const {
    executionDate,
    state,
    runType,
    lastSchedulingDecision,
    dataIntervalStart,
    dataIntervalEnd,
    startDate,
    endDate,
  } = run;
  const detailsParams = new URLSearchParams({
    run_id: runId,
  }).toString();
  const graphParams = new URLSearchParams({
    execution_date: executionDate,
  }).toString();
  const graphLink = appendSearchParams(graphUrl, graphParams);
  const detailsLink = appendSearchParams(dagRunDetailsUrl, detailsParams);

  return (
    <Box py="4px">
      <Flex justifyContent="space-between" alignItems="center">
        <Button as={Link} variant="ghost" colorScheme="blue" href={detailsLink}>DAG Run Details</Button>
        <Button as={Link} variant="ghost" colorScheme="blue" href={graphLink} leftIcon={<MdOutlineAccountTree />}>
          Graph
        </Button>
        <MarkFailedRun dagId={dagId} runId={runId} />
        <MarkSuccessRun dagId={dagId} runId={runId} />
      </Flex>
      <Divider my={3} />
      <Flex justifyContent="flex-end" alignItems="center">
        <Text fontWeight="bold" mr={2}>Re-run:</Text>
        <ClearRun dagId={dagId} runId={runId} />
        <QueueRun dagId={dagId} runId={runId} />
      </Flex>
      <Divider my={3} />
      <Flex alignItems="center">
        <Text as="strong">Status:</Text>
        <SimpleStatus state={state} mx={2} />
        {state || 'no status'}
      </Flex>
      <br />
      <Text whiteSpace="nowrap">
        Run Id:
        {' '}
        <ClipboardText value={runId} />
      </Text>
      <Text>
        Run Type:
        {' '}
        {runType === 'manual' && <MdPlayArrow style={{ display: 'inline' }} />}
        {runType === 'backfill' && <RiArrowGoBackFill style={{ display: 'inline' }} />}
        {runType === 'scheduled' && <MdOutlineSchedule style={{ display: 'inline' }} />}
        {runType}
      </Text>
      <Text>
        Duration:
        {' '}
        {formatDuration(getDuration(startDate, endDate))}
      </Text>
      {lastSchedulingDecision && (
      <Text>
        Last Scheduling Decision:
        {' '}
        <Time dateTime={lastSchedulingDecision} />
      </Text>
      )}
      <br />
      {startDate && (
      <Text>
        Started:
        {' '}
        <Time dateTime={startDate} />
      </Text>
      )}
      {endDate && (
      <Text>
        Ended:
        {' '}
        <Time dateTime={endDate} />
      </Text>
      )}
      {dataIntervalStart && dataIntervalEnd && (
        <>
          <br />
          <Text as="strong">Data Interval:</Text>
          <Text>
            Start:
            {' '}
            <Time dateTime={dataIntervalStart} />
          </Text>
          <Text>
            End:
            {' '}
            <Time dateTime={dataIntervalEnd} />
          </Text>
        </>
      )}
    </Box>
  );
};

export default DagRun;
