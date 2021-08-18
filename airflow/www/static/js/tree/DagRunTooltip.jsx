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
import { MdPlayArrow } from 'react-icons/md';

import { formatDateTime } from '../datetime_utils';
import { formatDuration } from './utils';

const DagRunTooltip = ({
  dagRun: {
    state, runId, duration, dataIntervalStart, dataIntervalEnd, startDate, endDate, runType,
  },
}) => (
  <Box fontSize={12}>
    <br />
    <Text>
      <Text as="strong">Status:</Text>
      {' '}
      {state || 'no status'}
    </Text>
    <br />
    <Text whiteSpace="nowrap">
      Run Id:
      {' '}
      {runId}
    </Text>
    <Text>
      Run Type:
      {' '}
      {runType === 'manual' && <MdPlayArrow style={{ display: 'inline' }} />}
      {runType}
    </Text>
    <Text>
      Duration:
      {' '}
      {formatDuration(duration)}
    </Text>
    <br />
    <Text as="strong">Data Interval:</Text>
    <Text>
      Start:
      {' '}
      {formatDateTime(dataIntervalStart)}
    </Text>
    <Text>
      End:
      {' '}
      {formatDateTime(dataIntervalEnd)}
    </Text>
    <br />
    <Text as="strong">UTC</Text>
    <Text>
      Started:
      {' '}
      {formatDateTime(moment.utc(startDate))}
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
      {formatDateTime(startDate)}
    </Text>
    <Text>
      Ended:
      {' '}
      {endDate && formatDateTime(endDate)}
    </Text>
    <br />
  </Box>
);

export default DagRunTooltip;
