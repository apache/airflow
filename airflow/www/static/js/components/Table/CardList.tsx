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

import React, { useEffect } from "react";
import {
  Flex,
  IconButton,
  Text,
  SimpleGrid,
  Box,
  Progress,
  Skeleton,
  BoxProps,
  SimpleGridProps,
} from "@chakra-ui/react";
import {
  useTable,
  useSortBy,
  usePagination,
  useRowSelect,
  Column,
  SortingRule,
  Row,
} from "react-table";
import { MdKeyboardArrowLeft, MdKeyboardArrowRight } from "react-icons/md";
import { flexRender } from "@tanstack/react-table";

export interface CardDef<TData> {
  card: (props: { row: TData }) => any;
  gridProps?: SimpleGridProps;
  meta?: {
    customSkeleton?: JSX.Element;
  };
}

interface TableProps<TData> extends BoxProps {
  data: object[];
  columns: Column<object>[];
  manualPagination?: {
    totalEntries: number;
    offset: number;
    setOffset: (offset: number) => void;
  };
  manualSort?: {
    sortBy: SortingRule<object>[];
    setSortBy: (sortBy: SortingRule<object>[]) => void;
    initialSortBy?: SortingRule<object>[];
  };
  pageSize?: number;
  isLoading?: boolean;
  selectRows?: (selectedRows: number[]) => void;
  onRowClicked?: (row: Row<object>, e: unknown) => void;
  cardDef: CardDef<TData>;
}

export const CardList = <TData extends any>({
  data,
  cardDef,
  columns,
  manualPagination,
  manualSort,
  pageSize = 25,
  isLoading = false,
  selectRows,
  onRowClicked,
  ...otherProps
}: TableProps<TData>) => {
  const { totalEntries, offset, setOffset } = manualPagination || {};

  const pageCount = totalEntries
    ? Math.ceil(totalEntries / pageSize) || 1
    : data.length;

  const lowerCount = (offset || 0) + 1;
  const upperCount = lowerCount + data.length - 1;

  // Don't show row selection if selectRows doesn't exist
  const selectProps = selectRows ? [useRowSelect] : [];

  const {
    prepareRow,
    page,
    canPreviousPage,
    canNextPage,
    nextPage,
    previousPage,
    selectedFlatRows,
    state: { pageIndex, sortBy, selectedRowIds },
  } = useTable(
    {
      columns,
      data,
      pageCount,
      manualPagination: !!manualPagination,
      manualSortBy: !!manualSort,
      disableMultiSort: !!manualSort, // API only supporting ordering by a single column
      initialState: {
        pageIndex: offset ? offset / pageSize : 0,
        pageSize,
        sortBy: manualSort?.initialSortBy || [],
      },
    },
    useSortBy,
    usePagination,
    ...selectProps
  );

  const handleNext = () => {
    nextPage();
    if (setOffset) setOffset((pageIndex + 1) * pageSize);
  };

  const handlePrevious = () => {
    previousPage();
    if (setOffset) setOffset((pageIndex - 1 || 0) * pageSize);
  };

  // When the sortBy state changes we need to manually call setSortBy
  useEffect(() => {
    if (manualSort) {
      manualSort.setSortBy(sortBy);
    }
  }, [sortBy, manualSort]);

  useEffect(() => {
    if (selectRows) {
      // @ts-ignore
      selectRows(selectedFlatRows.map((row) => row.original.mapIndex));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRowIds, selectRows]);

  const defaultGridProps = { column: { base: 1 }, spacing: 0 };

  return (
    <>
      <Box overflow="auto" width="100%" {...otherProps}>
        <Progress
          size="xs"
          isIndeterminate
          visibility={isLoading ? "visible" : "hidden"}
        />
        {!isLoading && !page.length && (
          <Text fontSize="small">No data found</Text>
        )}
        <SimpleGrid {...defaultGridProps}>
          {page.map((row) => {
            prepareRow(row);
            return (
              <Box
                key={row.id}
                _hover={onRowClicked && { cursor: "pointer" }}
                onClick={
                  onRowClicked && !isLoading
                    ? (e) => onRowClicked(row, e)
                    : undefined
                }
              >
                {isLoading && (
                  <Skeleton
                    data-testid="skeleton"
                    height={80}
                    width="100%"
                    display="inline-block"
                  />
                )}
                {!isLoading &&
                  flexRender(cardDef.card, {
                    row: row.original as unknown as TData,
                  })}
              </Box>
            );
          })}
        </SimpleGrid>
      </Box>
      {(canPreviousPage || canNextPage) && (
        <Flex alignItems="center" justifyContent="flex-start" my={4}>
          <IconButton
            variant="ghost"
            onClick={handlePrevious}
            disabled={!canPreviousPage}
            aria-label="Previous Page"
            title="Previous Page"
            icon={<MdKeyboardArrowLeft />}
          />
          <IconButton
            variant="ghost"
            onClick={handleNext}
            disabled={!canNextPage}
            aria-label="Next Page"
            title="Next Page"
            icon={<MdKeyboardArrowRight />}
          />
          <Text>
            {lowerCount}-{upperCount}
            {" of "}
            {totalEntries || data.length}
          </Text>
        </Flex>
      )}
    </>
  );
};
