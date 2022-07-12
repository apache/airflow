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

import { formatDuration } from 'app/datetime_utils';
import Time from 'grid/components/Time';

import type { RunWithDuration } from './index';

interface Props {
  dagRun: RunWithDuration;
}

const DagRunTooltip = ({
  dagRun: {
    state, duration, dataIntervalStart, executionDate, runType,
  },
}: Props) => (
  <Box py="2px">
    <Text>
      Status:
      {' '}
      {state || 'no status'}
    </Text>
    <Text whiteSpace="nowrap">
      Run:
      {' '}
      <Time dateTime={dataIntervalStart || executionDate} />
    </Text>
    <Text>
      Duration:
      {' '}
      {formatDuration(duration)}
    </Text>
    <Text>
      Type:
      {' '}
      {runType}
    </Text>
  </Box>
);

export default DagRunTooltip;
