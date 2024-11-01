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
  Button,
  Table as ChakraTable,
  TableContainer,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@chakra-ui/react";
import {
  flexRender,
  type Row,
  type Table as TanStackTable,
} from "@tanstack/react-table";
import React, { Fragment } from "react";
import {
  TiArrowSortedDown,
  TiArrowSortedUp,
  TiArrowUnsorted,
} from "react-icons/ti";

type DataTableProps<TData> = {
  readonly renderSubComponent?: (props: {
    row: Row<TData>;
  }) => React.ReactElement;
  readonly table: TanStackTable<TData>;
};

export const TableList = <TData,>({
  renderSubComponent,
  table,
}: DataTableProps<TData>) => (
  <TableContainer maxH="calc(100vh - 10rem)" overflowY="auto">
    <ChakraTable colorScheme="blue">
      <Thead bg="chakra-body-bg" position="sticky" top={0} zIndex={1}>
        {table.getHeaderGroups().map((headerGroup) => (
          <Tr key={headerGroup.id}>
            {headerGroup.headers.map(
              ({ colSpan, column, getContext, id, isPlaceholder }) => {
                const sort = column.getIsSorted();
                const canSort = column.getCanSort();
                const text = flexRender(column.columnDef.header, getContext());

                let rightIcon;

                if (canSort) {
                  if (sort === "desc") {
                    rightIcon = (
                      <TiArrowSortedDown
                        aria-label="sorted descending"
                        size="1em"
                        style={{ display: "inline" }}
                      />
                    );
                  } else if (sort === "asc") {
                    rightIcon = (
                      <TiArrowSortedUp
                        aria-label="sorted ascending"
                        size="1em"
                        style={{ display: "inline" }}
                      />
                    );
                  } else {
                    rightIcon = (
                      <TiArrowUnsorted
                        aria-label="unsorted"
                        size="1em"
                        style={{ display: "inline" }}
                      />
                    );
                  }

                  return (
                    <Th colSpan={colSpan} key={id} whiteSpace="nowrap">
                      {isPlaceholder ? undefined : (
                        <Button
                          aria-label="sort"
                          fontSize="inherit"
                          fontWeight="inherit"
                          isDisabled={!canSort}
                          minWidth={0}
                          onClick={column.getToggleSortingHandler()}
                          padding={0}
                          rightIcon={rightIcon}
                          textTransform="inherit"
                          variant="unstyled"
                        >
                          {text}
                        </Button>
                      )}
                    </Th>
                  );
                }

                return (
                  <Th colSpan={colSpan} key={id} whiteSpace="nowrap">
                    {isPlaceholder ? undefined : text}
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
  </TableContainer>
);
