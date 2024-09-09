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

"use client";

import {
  ColumnDef,
  Table as TanStackTable,
  flexRender,
  getCoreRowModel,
  getExpandedRowModel,
  getPaginationRowModel,
  OnChangeFn,
  PaginationState,
  Row,
  useReactTable,
} from "@tanstack/react-table";
import {
  Box,
  Button,
  Table as ChakraTable,
  TableContainer,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@chakra-ui/react";
import React, { Fragment } from "react";

type DataTableProps<TData> = {
  data: TData[];
  total?: number;
  columns: ColumnDef<TData>[];
  renderSubComponent?: (props: {
    row: Row<TData>;
  }) => React.ReactElement | null;
  getRowCanExpand?: (row: Row<TData>) => boolean;
  pagination: PaginationState;
  setPagination: OnChangeFn<PaginationState>;
};

type PaginatorProps<TData> = {
  table: TanStackTable<TData>;
};

const TablePaginator = <TData,>({ table }: PaginatorProps<TData>) => {
  const pageInterval = 3;
  const currentPageNumber = table.getState().pagination.pageIndex + 1;
  const startPageNumber = Math.max(1, currentPageNumber - pageInterval);
  const endPageNumber = Math.min(
    table.getPageCount(),
    startPageNumber + pageInterval * 2
  );
  const pageNumbers = [];

  for (let index = startPageNumber; index <= endPageNumber; index++) {
    pageNumbers.push(
      <Button
        borderRadius={0}
        key={index}
        isDisabled={index === currentPageNumber}
        onClick={() => table.setPageIndex(index - 1)}
      >
        {index}
      </Button>
    );
  }

  return (
    <Box mt={2} mb={2}>
      <Button
        borderRadius={0}
        onClick={() => table.firstPage()}
        isDisabled={!table.getCanPreviousPage()}
      >
        {"<<"}
      </Button>

      <Button
        borderRadius={0}
        onClick={() => table.previousPage()}
        isDisabled={!table.getCanPreviousPage()}
      >
        {"<"}
      </Button>
      {pageNumbers}
      <Button
        borderRadius={0}
        onClick={() => table.nextPage()}
        isDisabled={!table.getCanNextPage()}
      >
        {">"}
      </Button>
      <Button
        borderRadius={0}
        onClick={() => table.lastPage()}
        isDisabled={!table.getCanNextPage()}
      >
        {">>"}
      </Button>
    </Box>
  );
};

export function DataTable<TData>({
  data,
  total = 0,
  columns,
  renderSubComponent = () => null,
  getRowCanExpand = () => false,
  pagination,
  setPagination,
}: DataTableProps<TData>) {
  const table = useReactTable({
    data,
    columns,
    getRowCanExpand,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    onPaginationChange: setPagination,
    rowCount: total,
    manualPagination: true,
    state: {
      pagination,
    },
  });

  return (
    <TableContainer>
      <ChakraTable variant="striped">
        <Thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <Tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                return (
                  <Th key={header.id} colSpan={header.colSpan}>
                    {header.isPlaceholder ? null : (
                      <div>
                        {flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )}
                      </div>
                    )}
                  </Th>
                );
              })}
            </Tr>
          ))}
        </Thead>
        <Tbody>
          {table.getRowModel().rows.map((row) => {
            return (
              <Fragment key={row.id}>
                <Tr>
                  {/* first row is a normal row */}
                  {row.getVisibleCells().map((cell) => {
                    return (
                      <Td key={cell.id}>
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext()
                        )}
                      </Td>
                    );
                  })}
                </Tr>
                {row.getIsExpanded() && (
                  <Tr>
                    {/* 2nd row is a custom 1 cell row */}
                    <Td colSpan={row.getVisibleCells().length}>
                      {renderSubComponent({ row })}
                    </Td>
                  </Tr>
                )}
              </Fragment>
            );
          })}
        </Tbody>
      </ChakraTable>
      <TablePaginator table={table} />
    </TableContainer>
  );
}
