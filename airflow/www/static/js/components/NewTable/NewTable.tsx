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

/*
 * Custom wrapper of react-table using Chakra UI components
 */

import React, { useRef, useCallback } from "react";
import {
  Table as ChakraTable,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  useColorModeValue,
  TableProps as ChakraTableProps,
  HStack,
  Box,
  ButtonGroup,
  Button,
  Spinner,
} from "@chakra-ui/react";
import {
  useReactTable,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  PaginationState,
  SortingState,
  flexRender,
  ColumnDef,
  TableState as ReactTableState,
  RowData,
  OnChangeFn,
  Updater,
  Table as ReactTable,
} from "@tanstack/react-table";
import { MdKeyboardArrowLeft, MdKeyboardArrowRight } from "react-icons/md";
import {
  TiArrowUnsorted,
  TiArrowSortedDown,
  TiArrowSortedUp,
} from "react-icons/ti";
import { isEqual, pick } from "lodash";

import createSkeleton from "./createSkeleton";

export interface TableProps<TData extends RowData> extends ChakraTableProps {
  data: TData[];
  columns: ColumnDef<TData, any>[];
  initialState?: TableState;
  onStateChange?: (state: TableState) => void;
  resultCount?: number;
  isLoading?: boolean;
  isFetching?: boolean;
  onRowClicked?: (row: any, e: unknown) => void;
  skeletonCount?: number;
}

const normalizeState = (state: ReactTableState): TableState =>
  pick(state, "pagination", "sorting", "globalFilter");

export interface TableState {
  pagination: PaginationState;
  sorting: SortingState;
}

export const NewTable = <TData extends RowData>({
  data,
  columns,
  initialState,
  onStateChange,
  resultCount,
  isLoading = false,
  isFetching = false,
  onRowClicked,
  skeletonCount = 10,
}: TableProps<TData>) => {
  // ref current table instance so that we can "compute" the state
  const ref = useRef<{ tableRef: ReactTable<TData> | undefined }>({
    tableRef: undefined,
  });

  const oddColor = useColorModeValue("gray.50", "gray.900");
  const hoverColor = useColorModeValue("gray.100", "gray.700");

  const handleStateChange = useCallback<OnChangeFn<ReactTableState>>(
    (updater: Updater<ReactTableState>) => {
      // this is why we need ref, the state is NOT pass out as plain object but a updater function.
      if (ref.current.tableRef && onStateChange) {
        // nah, this is a ugly piece of code.
        const current = ref.current.tableRef.getState();
        const nextState =
          typeof updater === "function" ? updater(current) : updater;
        const old = normalizeState(current);
        const updated = normalizeState(nextState);
        // react table call onStateChange on every update even the state does not change.
        // so we need to do a quick compare here.
        if (!isEqual(old, updated)) onStateChange(updated);
      }
    },
    [onStateChange]
  );

  const tableInstance = useReactTable({
    columns,
    data,
    initialState: {
      ...initialState,
      pagination: {
        pageIndex: 0,
        pageSize: 25, // default page size to 25
        ...initialState?.pagination,
      },
    },
    ...(isLoading ? createSkeleton(skeletonCount, columns) : {}),
    rowCount: resultCount,
    manualPagination: true,
    manualSorting: true,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    onStateChange: handleStateChange,
  });

  ref.current.tableRef = tableInstance;

  const total = resultCount || data.length;
  const { pageIndex, pageSize } = tableInstance.getState().pagination;
  const lowerCount = Math.min(pageIndex * pageSize + 1, total);
  const upperCount = Math.min(
    (pageIndex + 1) * pageSize,
    pageIndex * pageSize + data.length,
    total
  );
  const canPrevious = tableInstance.getCanPreviousPage();
  const canNext = tableInstance.getCanNextPage() && data.length === pageSize;
  const handlePrevious = () => {
    tableInstance.previousPage();
  };
  const handleNext = () => {
    tableInstance.nextPage();
  };

  return (
    <>
      <ChakraTable>
        <Thead>
          {tableInstance.getHeaderGroups().map((headerGroup) => (
            <Tr key={headerGroup.id}>
              {headerGroup.headers.map(
                ({ id, colSpan, column, isPlaceholder, getContext }) => {
                  const sort = column.getIsSorted();
                  const canSort = column.getCanSort();
                  return (
                    <Th
                      key={id}
                      colSpan={colSpan}
                      whiteSpace="nowrap"
                      cursor={column.getCanSort() ? "pointer" : undefined}
                      onClick={column.getToggleSortingHandler()}
                    >
                      {isPlaceholder
                        ? null
                        : flexRender(column.columnDef.header, getContext())}
                      {canSort && !sort && (
                        <TiArrowUnsorted
                          aria-label="unsorted"
                          style={{ display: "inline" }}
                          size="1em"
                        />
                      )}
                      {canSort &&
                        sort &&
                        (sort === "desc" ? (
                          <TiArrowSortedDown
                            aria-label="sorted descending"
                            style={{ display: "inline" }}
                            size="1em"
                          />
                        ) : (
                          <TiArrowSortedUp
                            aria-label="sorted ascending"
                            style={{ display: "inline" }}
                            size="1em"
                          />
                        ))}
                    </Th>
                  );
                }
              )}
            </Tr>
          ))}
        </Thead>
        <Tbody>
          {tableInstance.getRowModel().rows.map((row) => (
            <Tr
              key={row.id}
              _odd={{ backgroundColor: oddColor }}
              _hover={
                onRowClicked && {
                  backgroundColor: hoverColor,
                  cursor: "pointer",
                }
              }
              onClick={
                onRowClicked ? (e: unknown) => onRowClicked(row, e) : undefined
              }
            >
              {row.getVisibleCells().map((cell) => (
                <Td key={cell.id} py={3}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </Td>
              ))}
            </Tr>
          ))}
        </Tbody>
      </ChakraTable>
      <HStack spacing={4} mt={4}>
        {upperCount && (
          <Box fontSize="sm">
            {lowerCount}-{upperCount}
            {upperCount !== total && ` out of ${total} total`}
          </Box>
        )}
        {(canPrevious || canNext) && (
          <ButtonGroup size="sm" isAttached variant="outline">
            <Button
              leftIcon={<MdKeyboardArrowLeft />}
              colorScheme="gray"
              aria-label="Previous Page"
              isDisabled={!canPrevious}
              onClick={handlePrevious}
            >
              Prev
            </Button>
            <Button
              rightIcon={<MdKeyboardArrowRight />}
              colorScheme="gray"
              aria-label="Next Page"
              isDisabled={!canNext}
              onClick={handleNext}
            >
              Next
            </Button>
            {isFetching && <Spinner ml={2} mt={4} size="sm" />}
          </ButtonGroup>
        )}
      </HStack>
    </>
  );
};
