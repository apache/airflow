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

import React, { useMemo } from 'react';
import {
  Table, Thead, Tbody, Tr, Th, Td, chakra, Alert, AlertIcon,
} from '@chakra-ui/react';
import {
  useTable, useSortBy, Column,
} from 'react-table';
import { MdArrowDropDown, MdArrowDropUp } from 'react-icons/md';

import { defaultDags } from 'api/defaults';
import { useDags } from 'api';
import {
  DagName, PauseToggle, TriggerDagButton, DagTag,
} from './Row';

const PipelinesTable: React.FC = () => {
  const { data: { dags } = defaultDags, isLoading, error } = useDags();
  const data = useMemo(
    () => dags.map((d) => ({
      ...d,
      tags: d.tags.map((tag) => <DagTag tag={tag} key={tag.name} />),
      dagId: <DagName dagId={d.dagId} />,
      trigger: <TriggerDagButton dagId={d.dagId} />,
      isPaused: <PauseToggle dagId={d.dagId} isPaused={d.isPaused} />,
    })),
    [dags],
  );

  const columns = useMemo<Column<any>[]>(
    () => [
      {
        accessor: 'isPaused',
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
        accessor: 'trigger',
      },
    ],
    [],
  );

  const {
    getTableProps,
    getTableBodyProps,
    allColumns,
    rows,
    prepareRow,
  } = useTable({ columns, data }, useSortBy);

  return (
    <>
      {error && (
      <Alert status="error" my="4" key={error.message}>
        <AlertIcon />
        {error.message}
      </Alert>
      )}
      <Table {...getTableProps()}>
        <Thead>
          <Tr>
            {allColumns.map((column) => (
              <Th
                {...column.getHeaderProps(column.getSortByToggleProps())}
              >
                {column.render('Header')}
                <chakra.span pl="2">
                  {column.isSorted && (
                    column.isSortedDesc ? (
                      <MdArrowDropDown aria-label="sorted descending" style={{ display: 'inline' }} size="2em" />
                    ) : (
                      <MdArrowDropUp aria-label="sorted ascending" style={{ display: 'inline' }} size="2em" />
                    )
                  )}
                </chakra.span>
              </Th>

            ))}
          </Tr>
        </Thead>
        <Tbody {...getTableBodyProps()}>
          {isLoading && (
          <Tr>
            <Td colSpan={2}>Loading…</Td>
          </Tr>
          )}
          {(!isLoading && !dags.length) && (
          <Tr>
            <Td colSpan={2}>No Pipelines found.</Td>
          </Tr>
          )}
          {rows.map((row) => {
            prepareRow(row);
            return (
              <Tr {...row.getRowProps()}>
                {row.cells.map((cell) => (
                  <Td
                    {...cell.getCellProps()}
                  >
                    {cell.render('Cell')}
                  </Td>
                ))}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </>
  );
};

export default PipelinesTable;
