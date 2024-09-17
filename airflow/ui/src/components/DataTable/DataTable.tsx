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
import {
  Table as ChakraTable,
  TableContainer,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
  useColorModeValue,
} from "@chakra-ui/react";
import {
  flexRender,
  getCoreRowModel,
  getExpandedRowModel,
  getPaginationRowModel,
  useReactTable,
  type ColumnDef,
  type OnChangeFn,
  type TableState as ReactTableState,
  type Row,
  type Table as TanStackTable,
  type Updater,
} from "@tanstack/react-table";
import React, { Fragment, useCallback, useRef } from "react";
import {
  TiArrowSortedDown,
  TiArrowSortedUp,
  TiArrowUnsorted,
} from "react-icons/ti";

import { TablePaginator } from "./TablePaginator";
import type { TableState } from "./types";

type DataTableProps<TData> = {
  readonly columns: Array<ColumnDef<TData>>;
  readonly data: Array<TData>;
  readonly getRowCanExpand?: (row: Row<TData>) => boolean;
  readonly initialState?: TableState;
  readonly onStateChange?: (state: TableState) => void;
  readonly renderSubComponent?: (props: {
    row: Row<TData>;
  }) => React.ReactElement;
  readonly total?: number;
};

const defaultGetRowCanExpand = () => false;

// eslint-disable-next-line max-lines-per-function
export const DataTable = <TData,>({
  columns,
  data,
  getRowCanExpand = defaultGetRowCanExpand,
  initialState,
  onStateChange,
  renderSubComponent,
  total = 0,
}: DataTableProps<TData>) => {
  const ref = useRef<{ tableRef: TanStackTable<TData> | undefined }>({
    tableRef: undefined,
  });
  const handleStateChange = useCallback<OnChangeFn<ReactTableState>>(
    (updater: Updater<ReactTableState>) => {
      if (ref.current.tableRef && onStateChange) {
        const current = ref.current.tableRef.getState();
        const next = typeof updater === "function" ? updater(current) : updater;

        // Only use the controlled state
        const nextState = {
          pagination: next.pagination,
          sorting: next.sorting,
        };

        onStateChange(nextState);
      }
    },
    [onStateChange],
  );

  const table = useReactTable({
    columns,
    data,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getRowCanExpand,
    manualPagination: true,
    manualSorting: true,
    onStateChange: handleStateChange,
    rowCount: total,
    state: initialState,
  });

  ref.current.tableRef = table;

  const theadBg = useColorModeValue("white", "gray.800");

  return (
    <TableContainer maxH="calc(100vh - 10rem)" overflowY="auto">
      <ChakraTable colorScheme="blue">
        <Thead bg={theadBg} position="sticky" top={0}>
          {table.getHeaderGroups().map((headerGroup) => (
            <Tr key={headerGroup.id}>
              {headerGroup.headers.map(
                ({ colSpan, column, getContext, id, isPlaceholder }) => {
                  const sort = column.getIsSorted();
                  const canSort = column.getCanSort();

                  return (
                    <Th
                      colSpan={colSpan}
                      cursor={column.getCanSort() ? "pointer" : undefined}
                      key={id}
                      onClick={column.getToggleSortingHandler()}
                      whiteSpace="nowrap"
                    >
                      {isPlaceholder ? undefined : (
                        <>{flexRender(column.columnDef.header, getContext())}</>
                      )}
                      {canSort && sort === false ? (
                        <TiArrowUnsorted
                          aria-label="unsorted"
                          size="1em"
                          style={{ display: "inline" }}
                        />
                      ) : undefined}
                      {canSort && sort !== false ? (
                        sort === "desc" ? (
                          <TiArrowSortedDown
                            aria-label="sorted descending"
                            size="1em"
                            style={{ display: "inline" }}
                          />
                        ) : (
                          <TiArrowSortedUp
                            aria-label="sorted ascending"
                            size="1em"
                            style={{ display: "inline" }}
                          />
                        )
                      ) : undefined}
                    </Th>
                  );
                },
              )}
            </Tr>
          ))}
        </Thead>
        <Tbody>
          {table.getRowModel().rows.map((row) => (
            <Fragment key={row.id}>
              <Tr>
                {/* first row is a normal row */}
                {row.getVisibleCells().map((cell) => (
                  <Td key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </Td>
                ))}
              </Tr>
              {row.getIsExpanded() && (
                <Tr>
                  {/* 2nd row is a custom 1 cell row */}
                  <Td colSpan={row.getVisibleCells().length}>
                    {renderSubComponent?.({ row })}
                  </Td>
                </Tr>
              )}
            </Fragment>
          ))}
        </Tbody>
      </ChakraTable>
      <TablePaginator table={table} />
    </TableContainer>
  );
};
