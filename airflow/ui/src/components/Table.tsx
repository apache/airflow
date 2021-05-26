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

import React, { useEffect } from 'react';
import {
  Flex,
  Table as ChakraTable,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  chakra,
  IconButton,
  Text,
  useColorModeValue,
} from '@chakra-ui/react';
import {
  useTable, useSortBy, Column, usePagination, SortingRule,
} from 'react-table';
import {
  MdArrowDropDown, MdArrowDropUp, MdKeyboardArrowLeft, MdKeyboardArrowRight,
} from 'react-icons/md';

interface Props {
  data: any[];
  columns: Column<any>[];
  /*
   * manualPagination is when you need to do server-side pagination.
   * Leave blank for client-side only
  */
  manualPagination?: {
    offset: number;
    setOffset: (off: number) => void;
    totalEntries: number;
  };
  /*
   * setSortBy is for custom sorting such as server-side sorting
  */
  setSortBy?: (sortBy: SortingRule<object>[]) => void;
  pageSize?: number;
}

const Table: React.FC<Props> = ({
  data, columns, manualPagination, pageSize = 25, setSortBy,
}) => {
  const { totalEntries, offset, setOffset } = manualPagination || {};
  const oddColor = useColorModeValue('gray.50', 'gray.900');
  const hoverColor = useColorModeValue('gray.100', 'gray.700');

  const pageCount = totalEntries ? (Math.ceil(totalEntries / pageSize) || 1) : data.length;

  const {
    getTableProps,
    getTableBodyProps,
    allColumns,
    prepareRow,
    page,
    canPreviousPage,
    canNextPage,
    nextPage,
    previousPage,
    state: { pageIndex, sortBy },
  } = useTable(
    {
      columns,
      data,
      pageCount,
      manualPagination: !!manualPagination,
      manualSortBy: !!setSortBy,
      initialState: {
        pageIndex: offset ? offset / pageSize : 0,
        pageSize,
      },
    },
    useSortBy,
    usePagination,
  );

  const handleNext = () => {
    nextPage();
    if (setOffset) setOffset((pageIndex + 1) * pageSize);
  };

  const handlePrevious = () => {
    previousPage();
    if (setOffset) setOffset((pageIndex - 1 || 0) * pageSize);
  };

  useEffect(() => {
    if (setSortBy) setSortBy(sortBy);
  }, [sortBy, setSortBy]);

  return (
    <>
      <ChakraTable {...getTableProps()}>
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
          {!data.length && (
          <Tr>
            <Td colSpan={2}>No Data found.</Td>
          </Tr>
          )}
          {page.map((row) => {
            prepareRow(row);
            return (
              <Tr
                {...row.getRowProps()}
                _odd={{ backgroundColor: oddColor }}
                _hover={{ backgroundColor: hoverColor }}
              >
                {row.cells.map((cell) => (
                  <Td
                    {...cell.getCellProps()}
                    py={3}
                  >
                    {cell.render('Cell')}
                  </Td>
                ))}
              </Tr>
            );
          })}
        </Tbody>
      </ChakraTable>
      <Flex alignItems="center" justifyContent="flex-start" my={4}>
        <IconButton variant="ghost" onClick={handlePrevious} disabled={!canPreviousPage} aria-label="Previous Page">
          <MdKeyboardArrowLeft />
        </IconButton>
        <IconButton variant="ghost" onClick={handleNext} disabled={!canNextPage} aria-label="Next Page">
          <MdKeyboardArrowRight />
        </IconButton>
        <Text>
          {pageIndex + 1}
          {' of '}
          {pageCount}
        </Text>
      </Flex>
    </>
  );
};

export default Table;
