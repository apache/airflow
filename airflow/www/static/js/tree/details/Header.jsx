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
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Box,
  Heading,
} from '@chakra-ui/react';
import { MdPlayArrow } from 'react-icons/md';

import { formatDateTime } from '../../datetime_utils';
import { getMetaValue } from '../../utils';

const dagId = getMetaValue('dag_id');

const LabelValue = ({ label, value }) => (
  <Box position="relative">
    <Heading as="h5" size="sm" color="gray.300" position="absolute" top="-12px">{label}</Heading>
    <Heading as="h3" size="md">{value}</Heading>
  </Box>
);

const Header = ({
  selected: { taskId, runId, task },
  onSelect,
  dagRuns,
}) => {
  const dagRun = dagRuns.find((r) => r.runId === runId);
  let runLabel = dagRun ? formatDateTime(dagRun.dataIntervalEnd) : '';
  if (dagRun && dagRun.runType === 'manual') {
    runLabel = (
      <>
        <MdPlayArrow style={{ display: 'inline' }} />
        {runLabel}
      </>
    );
  }

  const isMapped = task && task.isMapped;

  return (
    <Breadcrumb color="gray.300">
      <BreadcrumbItem isCurrentPage={!runId && !taskId} mt="15px">
        <BreadcrumbLink onClick={() => onSelect({})} color="black">
          <LabelValue label="DAG" value={dagId} />
        </BreadcrumbLink>
      </BreadcrumbItem>
      {runId && (
        <BreadcrumbItem isCurrentPage={runId && !taskId} mt="15px">
          <BreadcrumbLink onClick={() => onSelect({ runId, dagRun })}   color="black">
            <LabelValue label="Run" value={runLabel} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
      {taskId && (
        <BreadcrumbItem isCurrentPage mt="15px">
          <BreadcrumbLink   color="black">
            <LabelValue label="Task" value={isMapped ? `${taskId} []` : taskId} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
    </Breadcrumb>
  );
};

export default Header;
