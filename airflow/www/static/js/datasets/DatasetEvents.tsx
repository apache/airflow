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
import { snakeCase } from 'lodash';
import type { SortingRule } from 'react-table';

import { useDatasetEvents } from 'src/api';
import {
  Table, TimeCell, TaskInstanceLink,
} from 'src/components/Table';

const Events = ({
  datasetId,
}: { datasetId: number }) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([{ id: 'timestamp', desc: true }]);

  const sort = sortBy[0];
  const orderBy = sort ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const {
    data: { datasetEvents = [], totalEntries = 0 },
    isLoading: isEventsLoading,
  } = useDatasetEvents({
    datasetId, limit, offset, orderBy,
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
  );
};

export default Events;
