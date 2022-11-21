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

import React, {
  useState, useMemo,
} from 'react';
import {
  Flex,
  Text,
  Box,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { Row, SortingRule } from 'react-table';

import { formatDuration, getDuration } from 'src/datetime_utils';
import { useMappedInstances } from 'src/api';
import { StatusWithNotes } from 'src/dag/StatusBox';
import { Table } from 'src/components/Table';
import Time from 'src/components/Time';

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  onRowClicked: (row: Row) => void;
}

const MappedInstances = ({
  dagId, runId, taskId, onRowClicked,
}: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([]);

  const sort = sortBy[0];

  const orderBy = sort && (sort.id === 'state' || sort.id === 'mapIndex') ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const {
    data: { taskInstances = [], totalEntries = 0 } = { taskInstances: [], totalEntries: 0 },
    isLoading,
  } = useMappedInstances({
    dagId, dagRunId: runId, taskId, limit, offset, orderBy,
  });

  const data = useMemo(() => taskInstances.map((mi) => ({
    ...mi,
    state: (
      <Flex alignItems="center">
        <StatusWithNotes
          state={mi.state === undefined || mi.state === 'none' ? null : mi.state}
          mx={2}
          containsNotes={!!mi.notes}
        />
        {mi.state || 'no status'}
      </Flex>
    ),
    duration: mi.duration && formatDuration(getDuration(mi.startDate, mi.endDate)),
    startDate: <Time dateTime={mi.startDate} />,
    endDate: <Time dateTime={mi.endDate} />,
  })), [taskInstances]);

  const columns = useMemo(
    () => [
      {
        Header: 'Map Index',
        accessor: 'mapIndex',
      },
      {
        Header: 'State',
        accessor: 'state',
      },
      {
        Header: 'Duration',
        accessor: 'duration',
        disableSortBy: true,
      },
      {
        Header: 'Start Date',
        accessor: 'startDate',
        disableSortBy: true,
      },
      {
        Header: 'End Date',
        accessor: 'endDate',
        disableSortBy: true,
      },
    ],
    [],
  );

  return (
    <Box>
      <br />
      <Text as="strong">Mapped Instances</Text>
      <Table
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
        pageSize={limit}
        manualSort={{
          setSortBy,
          sortBy,
        }}
        isLoading={isLoading}
        onRowClicked={onRowClicked}
      />
    </Box>
  );
};

export default MappedInstances;
