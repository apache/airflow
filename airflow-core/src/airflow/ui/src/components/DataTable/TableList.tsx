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
import { Button, Table } from "@chakra-ui/react";
import { flexRender, type Row, type Table as TanStackTable } from "@tanstack/react-table";
import React, { Fragment } from "react";
import { TiArrowSortedDown, TiArrowSortedUp, TiArrowUnsorted } from "react-icons/ti";

import FilterMenuButton from "./FilterMenuButton";

type DataTableProps<TData> = {
  readonly allowFiltering: boolean;
  readonly renderSubComponent?: (props: { row: Row<TData> }) => React.ReactElement;
  readonly table: TanStackTable<TData>;
};

export const TableList = <TData,>({ allowFiltering, renderSubComponent, table }: DataTableProps<TData>) => (
  <Table.Root data-testid="table-list" striped>
    <Table.Header bg="chakra-body-bg" position="sticky" top={0} zIndex={1}>
      {table.getHeaderGroups().map((headerGroup) => (
        <Table.Row key={headerGroup.id}>
          {headerGroup.headers.map(({ colSpan, column, getContext, id, isPlaceholder }, index) => {
            const sort = column.getIsSorted();
            const canSort = column.getCanSort();
            const text = flexRender(column.columnDef.header, getContext());
            let rightIcon;

            const showFilters = allowFiltering && index === headerGroup.headers.length - 1;

            if (canSort) {
              if (sort === "desc") {
                rightIcon = <TiArrowSortedDown aria-label="sorted descending" />;
              } else if (sort === "asc") {
                rightIcon = <TiArrowSortedUp aria-label="sorted ascending" />;
              } else {
                rightIcon = <TiArrowUnsorted aria-label="unsorted" />;
              }

              return (
                <Table.ColumnHeader colSpan={colSpan} key={id} whiteSpace="nowrap">
                  {isPlaceholder ? undefined : (
                    <Button
                      aria-label="sort"
                      disabled={!canSort}
                      onClick={column.getToggleSortingHandler()}
                      variant="plain"
                    >
                      {text}
                      {rightIcon}
                    </Button>
                  )}
                  {showFilters ? <FilterMenuButton table={table} /> : undefined}
                </Table.ColumnHeader>
              );
            }

            return (
              <Table.ColumnHeader colSpan={colSpan} key={id} whiteSpace="nowrap">
                {isPlaceholder ? undefined : text}
                {showFilters ? <FilterMenuButton table={table} /> : undefined}
              </Table.ColumnHeader>
            );
          })}
        </Table.Row>
      ))}
    </Table.Header>
    <Table.Body>
      {table.getRowModel().rows.map((row) => (
        <Fragment key={row.id}>
          <Table.Row>
            {/* first row is a normal row */}
            {row.getVisibleCells().map((cell) => (
              <Table.Cell key={cell.id}>
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </Table.Cell>
            ))}
          </Table.Row>
          {row.getIsExpanded() && (
            <Table.Row>
              {/* 2nd row is a custom 1 cell row */}
              <Table.Cell colSpan={row.getVisibleCells().length}>{renderSubComponent?.({ row })}</Table.Cell>
            </Table.Row>
          )}
        </Fragment>
      ))}
    </Table.Body>
  </Table.Root>
);
