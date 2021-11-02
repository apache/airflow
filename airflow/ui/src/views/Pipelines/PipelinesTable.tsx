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
  Alert,
  AlertIcon,
  Progress,
  Switch,
  IconButton,
} from '@chakra-ui/react';
import type { Column } from 'react-table';
import {
  MdPlayArrow,
} from 'react-icons/md';

import Table from 'components/Table';
import { defaultDags } from 'api/defaults';
import { useDags } from 'api';
import {
  DagName, PauseToggle, TriggerDagButton, DagTag,
} from './Row';

const getRandomInt = (max: number) => Math.floor(Math.random() * max);

// Generate 1-10 placeholder rows
const skeletonLoader = [...Array(getRandomInt(10) || 1)].map(() => ({
  active: <Switch isDisabled />,
  tags: '',
  dagId: <Progress size="lg" isIndeterminate data-testid="pipelines-loading" />,
  trigger: <IconButton size="sm" icon={<MdPlayArrow />} aria-label="Trigger Dag" disabled />,
}));

const LIMIT = 25;

const PipelinesTable: React.FC = () => {
  const [offset, setOffset] = useState(0);
  const {
    data: { dags, totalEntries } = defaultDags,
    isLoading,
    error,
  } = useDags({ limit: LIMIT, offset });

  // Show placeholders rows when data is loading for the first time
  const data = useMemo(
    () => (isLoading && !dags.length
      ? skeletonLoader
      : dags.map((d) => ({
        ...d,
        tags: d.tags.map((tag) => <DagTag tag={tag} key={tag.name} />),
        dagId: <DagName dagId={d.dagId} />,
        trigger: <TriggerDagButton dagId={d.dagId} />,
        active: <PauseToggle dagId={d.dagId} isPaused={d.isPaused} offset={offset} />,
      }))),
    [dags, isLoading, offset],
  );

  const columns = useMemo<Column<any>[]>(
    () => [
      {
        Header: 'Active',
        accessor: 'active',
        // Implement custom sort function because the data is a react component
        sortType: (rowA, rowB) => (rowA.original.isPaused && !rowB.original.isPaused ? 1 : -1),
      },
      {
        Header: 'Dag Id',
        accessor: 'dagId',
      },
      {
        Header: 'Tags',
        accessor: 'tags',
      },
      {
        disableSortBy: true,
        accessor: 'trigger',
      },
    ],
    [],
  );

  return (
    <>
      {error && (
      <Alert status="error" my="4" key={error.message}>
        <AlertIcon />
        {error.message}
      </Alert>
      )}
      <Table
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
      />
    </>
  );
};

export default PipelinesTable;
