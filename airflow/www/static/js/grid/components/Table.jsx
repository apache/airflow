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

import React, { useEffect, useRef, forwardRef } from 'react';
import {
  Flex,
  Table as ChakraTable,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  IconButton,
  Text,
  useColorModeValue,
  Checkbox,
} from '@chakra-ui/react';
import {
  useTable, useSortBy, usePagination, useRowSelect,
} from 'react-table';
import {
  MdKeyboardArrowLeft, MdKeyboardArrowRight,
} from 'react-icons/md';
import {
  TiArrowUnsorted, TiArrowSortedDown, TiArrowSortedUp,
} from 'react-icons/ti';

const IndeterminateCheckbox = forwardRef(
  ({ indeterminate, checked, ...rest }, ref) => {
    const defaultRef = useRef();
    const resolvedRef = ref || defaultRef;

    useEffect(() => {
      resolvedRef.current.indeterminate = indeterminate;
    }, [resolvedRef, indeterminate]);

    return (
      <Checkbox ref={resolvedRef} isChecked={checked} {...rest} />
    );
  },
);

const Table = ({
  data, columns, manualPagination, pageSize = 25, setSortBy, isLoading = false, selectRows,
}) => {
  const { totalEntries, offset, setOffset } = manualPagination || {};
  const oddColor = useColorModeValue('gray.50', 'gray.900');
  const hoverColor = useColorModeValue('gray.100', 'gray.700');

  const pageCount = totalEntries ? (Math.ceil(totalEntries / pageSize) || 1) : data.length;

  const lowerCount = (offset || 0) + 1;
  const upperCount = lowerCount + data.length - 1;

  // Don't show row selection if selectRows doesn't exist
  const selectProps = selectRows
    ? [useRowSelect,
      (hooks) => {
        hooks.visibleColumns.push((cols) => [
          {
            id: 'selection',
            Cell: ({ row }) => (
              <div>
                <IndeterminateCheckbox {...row.getToggleRowSelectedProps()} />
              </div>
            ),
          },
          ...cols,
        ]);
      }]
    : [];

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
    selectedFlatRows,
    state: { pageIndex, sortBy, selectedRowIds },
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
    ...selectProps,
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

  useEffect(() => {
    if (selectRows) selectRows(selectedFlatRows.map((row) => row.original.mapIndex));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRowIds, selectRows]);

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
                {column.isSorted && (
                  column.isSortedDesc ? (
                    <TiArrowSortedDown aria-label="sorted descending" style={{ display: 'inline' }} size="1em" />
                  ) : (
                    <TiArrowSortedUp aria-label="sorted ascending" style={{ display: 'inline' }} size="1em" />
                  )
                )}
                {(!column.isSorted && column.canSort) && (<TiArrowUnsorted aria-label="unsorted" style={{ display: 'inline' }} size="1em" />)}
              </Th>
            ))}
          </Tr>
        </Thead>
        <Tbody {...getTableBodyProps()}>
          {!data.length && !isLoading && (
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
          {lowerCount}
          -
          {upperCount}
          {' of '}
          {totalEntries || data.length}
        </Text>
      </Flex>
      )}
    </>
  );
};

export default Table;
