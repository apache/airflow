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
  Text,
  Link,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  IconButton,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { Row, SortingRule } from 'react-table';
import { MdClose, MdSearch } from 'react-icons/md';
import { useSearchParams } from 'react-router-dom';

import { useDatasets } from 'src/api';
import { Table, TimeCell } from 'src/components/Table';
import type { API } from 'src/types';
import { getMetaValue } from 'src/utils';

interface Props {
  onSelect: (datasetId: string) => void;
}

interface CellProps {
  cell: {
    value: any;
    row: {
      original: Record<string, any>;
    }
  }
}

const DetailCell = ({ cell: { row } }: CellProps) => {
  const { totalUpdates, uri } = row.original;
  return (
    <Box data-testid="dataset-list-item">
      <Text>{uri}</Text>
      <Text fontSize="sm" mt={2}>
        Total Updates:
        {' '}
        {totalUpdates}
      </Text>
    </Box>
  );
};

const SEARCH_PARAM = 'search';

const DatasetsList = ({ onSelect }: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [searchParams, setSearchParams] = useSearchParams();
  const search = decodeURIComponent(searchParams.get(SEARCH_PARAM) || '');
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([{ id: 'lastDatasetUpdate', desc: true }]);

  const sort = sortBy[0];
  const order = sort ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';
  const uri = search.length > 2 ? search : undefined;

  const { data: { datasets, totalEntries }, isLoading } = useDatasets({
    limit,
    offset,
    order,
    uri,
  });

  const columns = useMemo(
    () => [
      {
        Header: 'URI',
        accessor: 'uri',
        Cell: DetailCell,
      },
      {
        Header: 'Last Update',
        accessor: 'lastDatasetUpdate',
        Cell: TimeCell,
      },
    ],
    [],
  );

  const data = useMemo(
    () => datasets,
    [datasets],
  );
  const memoSort = useMemo(() => sortBy, [sortBy]);

  const onDatasetSelect = (row: Row<API.Dataset>) => {
    if (row.original.uri) onSelect(row.original.uri);
  };

  const docsUrl = getMetaValue('datasets_docs');

  const onSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    searchParams.set(SEARCH_PARAM, encodeURIComponent(e.target.value));
    setSearchParams(searchParams);
  };

  const onClear = () => {
    searchParams.delete(SEARCH_PARAM);
    setSearchParams(searchParams);
  };

  return (
    <Box>
      <Flex justifyContent="space-between" alignItems="center">
        <Heading mt={3} mb={2} fontWeight="normal" size="lg">
          Datasets
        </Heading>
      </Flex>
      {!datasets.length && !isLoading && !search && (
        <Text mb={4} data-testid="no-datasets-msg">
          Looks like you do not have any datasets yet. Check out the
          {' '}
          <Link color="blue" href={docsUrl} isExternal>docs</Link>
          {' '}
          to learn how to create a dataset.
        </Text>
      )}
      <InputGroup my={2} px={1}>
        <InputLeftElement pointerEvents="none">
          <MdSearch />
        </InputLeftElement>
        <Input
          placeholder="Search by URI..."
          value={search}
          onChange={onSearch}
        />
        {search.length > 0 && (
          <InputRightElement>
            <IconButton aria-label="Clear search" title="Clear search" icon={<MdClose />} variant="ghost" onClick={onClear} />
          </InputRightElement>
        )}
      </InputGroup>
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
          manualSort={{
            setSortBy,
            sortBy,
            initialSortBy: memoSort,
          }}
          pageSize={limit}
          onRowClicked={onDatasetSelect}
        />
      </Box>
    </Box>
  );
};

export default DatasetsList;
