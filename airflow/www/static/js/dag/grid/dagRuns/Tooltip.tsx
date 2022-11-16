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
import { startCase } from 'lodash';

import { formatDuration } from 'src/datetime_utils';
import Time from 'src/components/Time';
import { getDagRunLabel } from 'src/utils';
import { useGridData } from 'src/api';

import type { RunWithDuration } from './index';

interface Props {
  dagRun: RunWithDuration;
}

const DagRunTooltip = ({ dagRun }: Props) => {
  const { data: { ordering } } = useGridData();
  return (
    <Box py="2px">
      <Text>
        Status:
        {' '}
        {dagRun.state || 'no status'}
      </Text>
      <Text whiteSpace="nowrap">
        {startCase(ordering[0] || ordering[1])}
        {': '}
        <Time dateTime={getDagRunLabel({ dagRun, ordering })} />
      </Text>
      <Text>
        Duration:
        {' '}
        {formatDuration(dagRun.duration)}
      </Text>
      <Text>
        Type:
        {' '}
        {dagRun.runType}
      </Text>
      {dagRun.notes && (
        <Text>Contains a note</Text>
      )}
    </Box>
  );
};

export default DagRunTooltip;
