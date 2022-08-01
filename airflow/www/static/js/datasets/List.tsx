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
import {
  Box,
  Heading,
  Flex,
  Button,
  Link,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { Row, SortingRule } from 'react-table';

import { useDatasets } from 'src/api';
import { Table, TimeCell, CodeCell } from 'src/components/Table';
import type { API } from 'src/types';
import { MdOutlineAccountTree } from 'react-icons/md';

interface Props {
  onSelect: (datasetId: string) => void;
}

const DatasetsList = ({ onSelect }: Props) => {
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
        Cell: CodeCell,
      },
      {
        Header: 'Created At',
        accessor: 'createdAt',
        Cell: TimeCell,
      },
      {
        Header: 'Updated At',
        accessor: 'updatedAt',
        Cell: TimeCell,
      },
    ],
    [],
  );

  const data = useMemo(
    () => datasets,
    [datasets],
  );

  const onDatasetSelect = (row: Row<API.Dataset>) => {
    if (row.original.id) onSelect(row.original.id.toString());
  };

  return (
    <Box maxWidth="1500px">
      <Flex justifyContent="space-between" alignItems="center">
        <Heading mt={3} mb={2} fontWeight="normal" title="View Dag-Dataset Dependencies">
          Datasets
        </Heading>
        <Button
          as={Link}
          variant="outline"
          colorScheme="blue"
          href="/dag-dependencies"
          leftIcon={<MdOutlineAccountTree />}
        >
          Graph
        </Button>
      </Flex>
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
          manualSort={{
            setSortBy,
            sortBy,
          }}
          onRowClicked={onDatasetSelect}
        />
      </Box>
    </Box>
  );
};

export default DatasetsList;
