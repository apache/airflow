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
  Text,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { Row, SortingRule } from 'react-table';

import { useDatasets } from 'src/api';
import { Table } from 'src/components/Table';
import type { API } from 'src/types';
import { MdOutlineAccountTree } from 'react-icons/md';
import InfoTooltip from 'src/components/InfoTooltip';
import { getMetaValue } from 'src/utils';

interface Props {
  onSelect: (datasetId: string) => void;
}

const UpstreamHeader = () => (
  <Flex>
    <Text>Producing Tasks</Text>
    <InfoTooltip size={12} label="Number of tasks that will update this dataset." />
  </Flex>
);

const DownstreamHeader = () => (
  <Flex>
    <Text>Consuming DAGs</Text>
    <InfoTooltip size={12} label="Number of DAGs that will run based on updates to this dataset." />
  </Flex>
);

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
        Header: UpstreamHeader,
        accessor: 'producingTasks',
        Cell: ({ cell: { value } }: any) => value.length,
        disableSortBy: true,
      },
      {
        Header: DownstreamHeader,
        accessor: 'consumingDags',
        Cell: ({ cell: { value } }: any) => value.length,
        disableSortBy: true,
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

  const docsUrl = getMetaValue('datasets_docs');

  return (
    <Box maxWidth="1500px">
      <Flex justifyContent="space-between" alignItems="center">
        <Heading mt={3} mb={2} fontWeight="normal">
          Datasets
        </Heading>
        <Button
          as={Link}
          variant="outline"
          colorScheme="blue"
          href="/dag-dependencies"
          title="View Dag-Dataset Dependencies"
          leftIcon={<MdOutlineAccountTree />}
        >
          Graph
        </Button>
      </Flex>
      {!datasets.length && !isLoading && (
        <Text>
          Looks like you do not have any datasets yet. Check out the
          {' '}
          <Link color="blue" href={docsUrl} isExternal>docs</Link>
          {' '}
          to learn how to create a dataset.
        </Text>
      )}
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
