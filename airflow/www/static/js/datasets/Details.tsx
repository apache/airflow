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
  Box, Heading, Flex, Text, Spinner, Button, Link,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { SortingRule } from 'react-table';

import { useDatasetEvents, useDataset } from 'src/api';
import {
  Table, TimeCell, TaskInstanceLink,
} from 'src/components/Table';
import { ClipboardButton } from 'src/components/Clipboard';
import type { API } from 'src/types';
import InfoTooltip from 'src/components/InfoTooltip';
import { getMetaValue } from 'src/utils';

interface Props {
  datasetId: string;
  onBack: () => void;
}

const gridUrl = getMetaValue('grid_url');

const Details = ({
  dataset: {
    uri,
    upstreamTaskReferences,
    downstreamDagReferences,
  },
}: { dataset: API.Dataset }) => (
  <Box>
    <Heading my={2} fontWeight="normal">
      Dataset:
      {' '}
      {uri}
      <ClipboardButton value={uri} iconOnly ml={2} />
    </Heading>
    {upstreamTaskReferences && !!upstreamTaskReferences.length && (
    <Box mb={2}>
      <Flex alignItems="center">
        <Heading size="md" fontWeight="normal">Producing Tasks</Heading>
        <InfoTooltip label="Tasks that will update this dataset." size={14} />
      </Flex>
      {upstreamTaskReferences.map(({ dagId, taskId }) => (
        <Link
          key={`${dagId}.${taskId}`}
          color="blue.600"
          href={dagId ? gridUrl?.replace('__DAG_ID__', dagId) : ''}
          display="block"
        >
          {`${dagId}.${taskId}`}
        </Link>
      ))}
    </Box>
    )}
    {downstreamDagReferences && !!downstreamDagReferences.length && (
    <Box>
      <Flex alignItems="center">
        <Heading size="md" fontWeight="normal">Consuming DAGs</Heading>
        <InfoTooltip label="DAGs that depend on this dataset updating to trigger a run." size={14} />
      </Flex>
      {downstreamDagReferences.map(({ dagId }) => (
        <Link
          key={dagId}
          color="blue.600"
          href={dagId ? gridUrl?.replace('__DAG_ID__', dagId) : ''}
          display="block"
        >
          {dagId}
        </Link>
      ))}
    </Box>
    )}
  </Box>
);

const DatasetDetails = ({ datasetId, onBack }: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([{ id: 'timestamp', desc: true }]);

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
        Header: 'Source Task Instance',
        accessor: 'sourceTaskId',
        Cell: TaskInstanceLink,
      },
      {
        Header: 'When',
        accessor: 'timestamp',
        Cell: TimeCell,
      },
    ],
    [],
  );

  const data = useMemo(
    () => datasetEvents,
    [datasetEvents],
  );

  const memoSort = useMemo(() => sortBy, [sortBy]);

  return (
    <Box mt={[6, 3]} maxWidth="1500px">
      <Button onClick={onBack}>See all datasets</Button>
      {isLoading && <Spinner display="block" />}
      {!!dataset && (<Details dataset={dataset} />)}
      <Box>
        <Heading size="lg" mt={3} mb={2} fontWeight="normal">History</Heading>
        <Text>Whenever a DAG has updated this dataset.</Text>
      </Box>
      <Table
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
        manualSort={{
          setSortBy,
          sortBy,
          initialSortBy: memoSort,
        }}
        pageSize={limit}
        isLoading={isEventsLoading}
      />
    </Box>
  );
};

export default DatasetDetails;
