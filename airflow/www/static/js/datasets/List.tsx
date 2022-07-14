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

import React, { useMemo, useState } from 'react';
import { Box, Code, Heading } from '@chakra-ui/react';

import { useDatasets } from 'src/api';
import Table from 'src/components/Table';
import Time from 'src/components/Time';
import { snakeCase } from 'lodash';
import type { SortingRule } from 'react-table';

const DatasetsList = () => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([]);

  const sort = sortBy[0];
  const order = sort ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const { data: { datasets, totalEntries }, isLoading } = useDatasets({ limit, offset, order });

  const columns = useMemo(
    () => [
      {
        Header: 'URI',
        accessor: 'uri',
      },
      {
        Header: 'Extra',
        accessor: 'extra',
        disableSortBy: true,
      },
      {
        Header: 'Created At',
        accessor: 'createdAt',
      },
      {
        Header: 'Updated At',
        accessor: 'updatedAt',
      },
    ],
    [],
  );

  const data = useMemo(
    () => datasets.map((d) => ({
      ...d,
      extra: <Code>{d.extra}</Code>,
      createdAt: <Time dateTime={d.createdAt} />,
      updatedAt: <Time dateTime={d.updatedAt} />,
    })),
    [datasets],
  );

  return (
    <Box>
      <Heading mt={3} mb={2} fontWeight="normal">
        Datasets
      </Heading>
      <Box borderWidth={1}>
        <Table
          data={data}
          columns={columns}
          isLoading={isLoading}
          manualPagination={{
            offset,
            setOffset,
            totalEntries,
          }}
          pageSize={limit}
          setSortBy={setSortBy}
        />
      </Box>
    </Box>
  );
};

export default DatasetsList;
