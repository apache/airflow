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
import { Heading, HStack, Text } from "@chakra-ui/react";
import {
  getCoreRowModel,
  getExpandedRowModel,
  getPaginationRowModel,
  useReactTable,
  type VisibilityState,
  type OnChangeFn,
  type TableState as ReactTableState,
  type Row,
  type Table as TanStackTable,
  type Updater,
} from "@tanstack/react-table";
import React, { type ReactNode, useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";

import { CardList } from "src/components/DataTable/CardList";
import { TableList } from "src/components/DataTable/TableList";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import { createSkeletonMock } from "src/components/DataTable/skeleton";
import type { CardDef, MetaColumn, TableState } from "src/components/DataTable/types";
import { ProgressBar, Pagination, Toaster } from "src/components/ui";

type DataTableProps<TData> = {
  readonly allowFiltering?: boolean;
  readonly cardDef?: CardDef<TData>;
  readonly columns: Array<MetaColumn<TData>>;
  readonly data: Array<TData>;
  readonly displayMode?: "card" | "table";
  readonly errorMessage?: ReactNode | string;
  readonly getRowCanExpand?: (row: Row<TData>) => boolean;
  readonly initialState?: TableState;
  readonly isFetching?: boolean;
  readonly isLoading?: boolean;
  readonly modelName: string;
  readonly noRowsMessage?: ReactNode;
  readonly onDisplayToggleChange?: (mode: "card" | "table") => void;
  readonly onStateChange?: (state: TableState) => void;
  readonly renderSubComponent?: (props: { row: Row<TData> }) => React.ReactElement;
  readonly showDisplayToggle?: boolean;
  readonly showRowCountHeading?: boolean;
  readonly skeletonCount?: number;
  readonly total?: number;
};

const defaultGetRowCanExpand = () => false;

export const DataTable = <TData,>({
  allowFiltering,
  cardDef,
  columns,
  data,
  displayMode = "table",
  errorMessage,
  getRowCanExpand = defaultGetRowCanExpand,
  initialState,
  isFetching,
  isLoading,
  modelName,
  noRowsMessage,
  onDisplayToggleChange,
  onStateChange,
  showDisplayToggle,
  showRowCountHeading = true,
  skeletonCount = 10,
  total = 0,
}: DataTableProps<TData>) => {
  "use no memo"; // remove if https://github.com/TanStack/table/issues/5567 is resolved

  const { t: translate } = useTranslation(["common"]);
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
          columnVisibility: next.columnVisibility,
          pagination: next.pagination,
          sorting: next.sorting,
        };

        onStateChange(nextState);
      }
    },
    [onStateChange],
  );

  const [columnVisibility, setColumnVisibility] = useLocalStorage<VisibilityState>(
    `dataTable:${modelName}:columnVisibility`,
    initialState?.columnVisibility ?? {},
  );

  const rest = Boolean(isLoading) ? createSkeletonMock(displayMode, skeletonCount, columns) : {};

  const table = useReactTable({
    columns,
    data,
    enableHiding: true,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getRowCanExpand,
    manualPagination: true,
    manualSorting: true,
    onColumnVisibilityChange: setColumnVisibility,
    onStateChange: handleStateChange,
    rowCount: total,
    // We need to manually set the sort toggle buttons for undefined values
    sortDescFirst: false,
    state: { ...initialState, columnVisibility },
    ...rest,
  });

  ref.current.tableRef = table;

  const { rows } = table.getRowModel();

  const rowCount = table.getRowCount();
  const { setPageIndex } = table;

  const { pagination } = table.getState();
  const { pageIndex, pageSize } = pagination;

  const display = displayMode === "card" && Boolean(cardDef) ? "card" : "table";
  const hasRows = rows.length > 0;
  const hasPagination = initialState?.pagination !== undefined && (pageIndex !== 0 || rows.length !== total);

  // Default to show columns filter only if there are actually many columns displayed
  const showColumnsFilter = allowFiltering ?? columns.length > 5;

  const translateModelName = useCallback(
    (count: number) => translate(modelName, { count }),
    [modelName, translate],
  );
  const showRowCount = Boolean(
    showRowCountHeading && !Boolean(isLoading) && !Boolean(isFetching) && total > 0,
  );
  const noRowsModelName = translateModelName(0);

  const rowCountHeading = showRowCount ? (
    <Heading py={3} size="md">
      {`${total} ${translateModelName(total)}`}
    </Heading>
  ) : undefined;

  return (
    <>
      <ProgressBar size="xs" visibility={Boolean(isFetching) && !Boolean(isLoading) ? "visible" : "hidden"} />
      {showDisplayToggle && onDisplayToggleChange ? (
        <ToggleTableDisplay display={display} setDisplay={onDisplayToggleChange} />
      ) : undefined}
      <Toaster />
      {errorMessage}
      {rowCountHeading}
      {hasRows && display === "table" ? (
        <TableList allowFiltering={showColumnsFilter} table={table} />
      ) : undefined}
      {hasRows && display === "card" && cardDef !== undefined ? (
        <CardList cardDef={cardDef} isLoading={isLoading} rows={rows} />
      ) : undefined}
      {!hasRows && !Boolean(isLoading) && (
        <Text as="div" pl={4} pt={1}>
          {noRowsMessage ?? translate("noItemsFound", { modelName: noRowsModelName })}
        </Text>
      )}
      {hasPagination ? (
        <Pagination.Root
          count={rowCount}
          my={2}
          onPageChange={(page) => setPageIndex(page.page - 1)}
          page={pageIndex + 1}
          pageSize={pageSize}
          siblingCount={1}
        >
          <HStack>
            <Pagination.PrevTrigger data-testid="prev" />
            <Pagination.Items />
            <Pagination.NextTrigger data-testid="next" />
          </HStack>
        </Pagination.Root>
      ) : undefined}
    </>
  );
};
