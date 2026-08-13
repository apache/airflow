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
import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import {
  getCoreRowModel,
  getPaginationRowModel,
  type OnChangeFn,
  type TableState as ReactTableState,
  type Table as TanStackTable,
  type Updater,
  useReactTable,
  type VisibilityState,
} from "@tanstack/react-table";
import { type ReactNode, useCallback, useRef } from "react";
import { useTranslation } from "react-i18next";
import { HiChevronLeft, HiChevronRight } from "react-icons/hi2";
import { useLocalStorage } from "usehooks-ts";

import { CardList } from "src/components/DataTable/CardList";
import { FilterMenuButton } from "src/components/DataTable/FilterMenuButton";
import { TableList } from "src/components/DataTable/TableList";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import { createSkeletonMock } from "src/components/DataTable/skeleton";
import type { CardDef, MetaColumn, TableState } from "src/components/DataTable/types";
import { IconButton, Pagination, ProgressBar, Toaster } from "src/components/ui";

type DataTableProps<TData> = {
  /**
   * Table-level controls (sort selects, expand/collapse) rendered at the start of the header row's
   * right-hand group, before the columns menu and the display toggle. Pass `undefined` rather than
   * an empty fragment when there is nothing to show, otherwise the header row renders empty.
   */
  readonly actions?: ReactNode;
  readonly cardDef?: CardDef<TData>;
  readonly columns: Array<MetaColumn<TData>>;
  readonly data: Array<TData>;
  readonly displayMode?: "card" | "table";
  readonly errorMessage?: ReactNode | string;
  /** Rendered next to the row count heading, and on its own when the heading is hidden. */
  readonly headingExtra?: ReactNode;
  readonly hideRowCountHeading?: boolean;
  readonly initialState?: TableState;
  readonly isFetching?: boolean;
  readonly isLoading?: boolean;
  /**
   * i18n key naming the model this table lists — namespaced (`common:dagRun`) or bare for `common`.
   *
   * Pass the base key, never a plural variant: the heading resolves it as `t(modelName, { count })`,
   * so i18next appends `_one`/`_other` itself and `common:event_one` would render "Event" at every
   * count. Both variants must exist, and `_other` is also read on its own for tables with no `total`
   * — `modelName.test.ts` checks this for every call site.
   *
   * It also keys the persisted column visibility, so tables sharing a `modelName` share hidden
   * columns, and changing it resets that state for existing users.
   */
  readonly modelName: string;
  readonly nextCursor?: string | null;
  readonly noRowsMessage?: ReactNode;
  readonly onDisplayToggleChange?: (mode: "card" | "table") => void;
  readonly onStateChange?: (state: TableState) => void;
  readonly previousCursor?: string | null;
  /** Defaults to showing the column visibility menu only when there are many columns. */
  readonly showColumnsMenu?: boolean;
  readonly showDisplayToggle?: boolean;
  readonly skeletonCount?: number;
  readonly total?: number;
  readonly totalEntriesLimit?: number;
};

export const DataTable = <TData,>({
  actions,
  cardDef,
  columns,
  data,
  displayMode = "table",
  errorMessage,
  headingExtra,
  hideRowCountHeading,
  initialState,
  isFetching,
  isLoading,
  modelName,
  nextCursor,
  noRowsMessage,
  onDisplayToggleChange,
  onStateChange,
  previousCursor,
  showColumnsMenu,
  showDisplayToggle,
  skeletonCount = 10,
  total,
  totalEntriesLimit,
}: DataTableProps<TData>) => {
  "use no memo"; // remove if https://github.com/TanStack/table/issues/5567 is resolved

  const { i18n, t: translate } = useTranslation(["common"]);
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

  // An absent total means the endpoint gives no count (e.g. cursor pagination), which the heading
  // reflects by naming the model without a number. Everything else still needs a real number.
  const rowTotal = total ?? 0;

  // Card mode is only reachable with a cardDef; without one the table renders and needs table skeletons
  const display = displayMode === "card" && Boolean(cardDef) ? "card" : "table";
  const rest = Boolean(isLoading) ? createSkeletonMock(display, skeletonCount, columns) : {};

  const table = useReactTable({
    columns,
    data,
    enableHiding: true,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    manualPagination: true,
    manualSorting: true,
    onColumnVisibilityChange: setColumnVisibility,
    onStateChange: handleStateChange,
    rowCount: rowTotal,
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

  const hasNext = nextCursor !== undefined && nextCursor !== null;
  const hasPrevious = previousCursor !== undefined && previousCursor !== null;
  const hasCursorPagination = hasNext || hasPrevious;
  // Page count is derived from the total, so without one there is nothing to paginate over
  const hasOffsetPagination =
    !hasCursorPagination &&
    total !== undefined &&
    initialState?.pagination !== undefined &&
    (pageIndex !== 0 || rows.length !== rowTotal);

  const translateModelName = (count: number) => translate(modelName, { count });
  // During the initial load there is nothing to count yet
  const showRowCount = !Boolean(hideRowCountHeading) && !Boolean(isLoading);
  const noRowsNode = noRowsMessage ?? translate("noItemsFound", { modelName: translateModelName(0) });
  // i18next derives the plural form from the count, but in some languages (Russian) no integer count
  // ever selects `_other`, so read the count-free plural key directly instead of passing a stand-in.
  const pluralModelName = translate(`${modelName}_other`);
  // Cursor pagination reports the total capped at totalEntriesLimit, so a total that reaches the
  // cap means "at least this many" and is rendered as "N+".
  const isCapped = total !== undefined && totalEntriesLimit !== undefined && total >= totalEntriesLimit;

  // Default to showing the columns menu only if there are actually many columns displayed
  const renderColumnsMenu =
    display === "table" &&
    (showColumnsMenu ?? columns.length > 5) &&
    table.getAllLeafColumns().some((column) => column.getCanHide());
  const headingNode = showRowCount ? (
    <Heading size="md">
      {total === undefined
        ? pluralModelName
        : `${total.toLocaleString(i18n.language)}${isCapped ? "+" : ""} ${translateModelName(total)}`}
    </Heading>
  ) : undefined;
  const renderHeaderRow =
    headingNode !== undefined ||
    headingExtra !== undefined ||
    actions !== undefined ||
    renderColumnsMenu ||
    (Boolean(showDisplayToggle) && onDisplayToggleChange !== undefined);

  return (
    <Box display="flex" flex={1} flexDirection="column" minH={0} w="100%">
      <Toaster />
      {renderHeaderRow ? (
        <Flex
          alignItems="center"
          data-testid="data-table-header"
          gap={2}
          justifyContent="space-between"
          minH={10}
          mt={2} // This offsets the spacing below created by the ProgressBar
        >
          <HStack gap={2}>
            {headingNode}
            {headingExtra}
          </HStack>
          {/* Margin keeps the group right-aligned when nothing renders on the left */}
          <HStack gap={2} ms="auto">
            {actions}
            {renderColumnsMenu ? <FilterMenuButton table={table} /> : undefined}
            {showDisplayToggle && onDisplayToggleChange ? (
              <ToggleTableDisplay display={display} setDisplay={onDisplayToggleChange} />
            ) : undefined}
          </HStack>
        </Flex>
      ) : undefined}
      {errorMessage}
      <ProgressBar size="xs" visibility={Boolean(isFetching) && !Boolean(isLoading) ? "visible" : "hidden"} />
      <Box flex={1} minH={0} overflow="auto">
        {display === "card" && cardDef !== undefined ? (
          <CardList cardDef={cardDef} isLoading={isLoading} noRowsMessage={noRowsNode} rows={rows} />
        ) : (
          <TableList noRowsMessage={noRowsNode} table={table} />
        )}
      </Box>
      {hasOffsetPagination ? (
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
      {/* Pagination.Root is designed for offset-based pagination and requires count/page/pageSize.
          Cursor pagination doesn't have these values, and passing fake values causes
          incorrect disabled styling on the triggers. Use plain IconButtons instead. */}
      {hasCursorPagination && initialState ? (
        <HStack my={2}>
          <IconButton
            aria-label="Previous page"
            data-testid="prev"
            disabled={!hasPrevious}
            onClick={() => {
              if (onStateChange && previousCursor !== undefined && previousCursor !== null) {
                onStateChange({ ...initialState, cursor: previousCursor });
              }
            }}
          >
            <HiChevronLeft />
          </IconButton>
          <IconButton
            aria-label="Next page"
            data-testid="next"
            disabled={!hasNext}
            onClick={() => {
              if (onStateChange && nextCursor !== undefined && nextCursor !== null) {
                onStateChange({ ...initialState, cursor: nextCursor });
              }
            }}
          >
            <HiChevronRight />
          </IconButton>
        </HStack>
      ) : undefined}
    </Box>
  );
};
