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
  Box, Heading, Text, Code, Flex, Spinner, Button, Link,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { SortingRule } from 'react-table';

import Time from 'src/components/Time';
import { useDatasetEvents } from 'src/api';
import useDataset from 'src/api/useDataset';
import Table from 'src/components/Table';

interface Props {
  datasetId: string;
  onBack: () => void;
}

const TimeCell = ({ cell: { value } }: any) => <Time dateTime={value} />;
const GridLink = ({ cell: { value } }: any) => <Link color="blue.500" href={`/dags/${value}/grid`}>{value}</Link>;
const RunLink = ({ cell: { value, row } }: any) => {
  const { sourceDagId } = row.original;
  const url = `/dags/${sourceDagId}/grid?dag_run_id=${encodeURIComponent(value)}`;
  return (<Link color="blue.500" href={url}>{value}</Link>);
};
const TaskInstanceLink = ({ cell: { value, row } }: any) => {
  const { sourceRunId, sourceDagId } = row.original;
  const url = `/dags/${sourceDagId}/grid?dag_run_id=${encodeURIComponent(sourceRunId)}&task_id=${encodeURIComponent(value)}`;
  return (<Link color="blue.500" href={url}>{value}</Link>);
};

const DatasetDetails = ({ datasetId, onBack }: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([{ id: 'createdAt', desc: true }]);

  const sort = sortBy[0];
  const order = sort ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const { data: dataset, isLoading } = useDataset({ datasetId });
  const {
    data: { datasetEvents, totalEntries },
    isLoading: isEventsLoading,
  } = useDatasetEvents({
    datasetId, limit, offset, order,
  });

  const columns = useMemo(
    () => [
      {
        Header: 'Created At',
        accessor: 'createdAt',
        Cell: TimeCell,
      },
      {
        Header: 'Source DAG Id',
        accessor: 'sourceDagId',
        Cell: GridLink,
      },
      {
        Header: 'Source DAG Run Id',
        accessor: 'sourceRunId',
        Cell: RunLink,
      },
      {
        Header: 'Source Task Id',
        accessor: 'sourceTaskId',
        Cell: TaskInstanceLink,
      },
      {
        Header: 'Source Map Index',
        accessor: 'sourceMapIndex',
        Cell: ({ cell: { value } }) => (value > -1 ? value : null),
      },
    ],
    [],
  );

  const data = useMemo(
    () => datasetEvents,
    [datasetEvents],
  );

  return (
    <Box maxWidth="1500px">
      <Flex mt={3} justifyContent="space-between">
        {isLoading && <Box><Spinner /></Box>}
        {!!dataset && (
          <Box>
            <Heading mb={2} fontWeight="normal">
              Dataset
              {' '}
              {dataset.id}
            </Heading>
            <Text my={2}>
              URI:
              {' '}
              {dataset.uri}
            </Text>
            {!!dataset.extra && (
              <Flex>
                <Text mr={1}>Extra:</Text>
                <Code>{dataset.extra}</Code>
              </Flex>
            )}
            <Flex my={2}>
              <Text mr={1}>Updated At:</Text>
              <Time dateTime={dataset.updatedAt} />
            </Flex>
            <Flex my={2}>
              <Text mr={1}>Created At:</Text>
              <Time dateTime={dataset.createdAt} />
            </Flex>
          </Box>
        )}
        <Button onClick={onBack}>See all datasets</Button>
      </Flex>
      <Heading size="lg" mt={3} mb={2} fontWeight="normal">Dataset Events</Heading>
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
      {!isLoading && isEventsLoading && <Spinner />}
    </Box>
  );
};

export default DatasetDetails;
