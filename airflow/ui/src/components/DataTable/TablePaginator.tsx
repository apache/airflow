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
import { Box, Button } from "@chakra-ui/react";
import type { Table as TanStackTable } from "@tanstack/react-table";
import { useCallback } from "react";

type PaginatorProps<TData> = {
  readonly table: TanStackTable<TData>;
};

export const TablePaginator = <TData,>({ table }: PaginatorProps<TData>) => {
  const pageInterval = 3;
  const currentPageNumber = table.getState().pagination.pageIndex + 1;
  const startPageNumber = Math.max(1, currentPageNumber - pageInterval);
  const endPageNumber = Math.min(
    table.getPageCount(),
    startPageNumber + pageInterval * 2,
  );
  const pageNumbers = [];

  const setPageIndex = useCallback(
    (index: number) => () => table.setPageIndex(index - 1),
    [table],
  );

  for (let index = startPageNumber; index <= endPageNumber; index += 1) {
    pageNumbers.push(
      <Button
        borderRadius={0}
        isDisabled={index === currentPageNumber}
        key={index}
        onClick={setPageIndex(index)}
      >
        {index}
      </Button>,
    );
  }

  return (
    <Box mb={2} mt={2}>
      <Button
        borderRadius={0}
        isDisabled={!table.getCanPreviousPage()}
        onClick={table.firstPage}
      >
        {"<<"}
      </Button>

      <Button
        borderRadius={0}
        isDisabled={!table.getCanPreviousPage()}
        onClick={table.previousPage}
      >
        {"<"}
      </Button>
      {pageNumbers}
      <Button
        borderRadius={0}
        isDisabled={!table.getCanNextPage()}
        onClick={table.nextPage}
      >
        {">"}
      </Button>
      <Button
        borderRadius={0}
        isDisabled={!table.getCanNextPage()}
        onClick={table.lastPage}
      >
        {">>"}
      </Button>
    </Box>
  );
};
