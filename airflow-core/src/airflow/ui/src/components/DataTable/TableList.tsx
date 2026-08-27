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
import { Button, Icon, Table } from "@chakra-ui/react";
import { flexRender, type Table as TanStackTable } from "@tanstack/react-table";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { TiArrowSortedDown, TiArrowSortedUp, TiArrowUnsorted } from "react-icons/ti";

type TableListProps<TData> = {
  readonly noRowsMessage?: ReactNode;
  readonly table: TanStackTable<TData>;
};

export const TableList = <TData,>({ noRowsMessage, table }: TableListProps<TData>) => {
  "use no memo"; // remove if https://github.com/TanStack/table/issues/5567 is resolved
  const { t: translate } = useTranslation("components");
  const { rows } = table.getRowModel();

  return (
    <Table.Root data-testid="table-list" size="sm" striped>
      <Table.Header bg="chakra-body-bg" position="sticky" top={0} zIndex={1}>
        {table.getHeaderGroups().map((headerGroup) => (
          <Table.Row key={headerGroup.id}>
            {headerGroup.headers.map(({ colSpan, column, getContext, id, isPlaceholder }) => {
              const sort = column.getIsSorted();
              const canSort = column.getCanSort();
              const text = flexRender(column.columnDef.header, getContext());
              let rightIcon;

              if (canSort) {
                if (sort === "desc") {
                  rightIcon = (
                    <Icon aria-label={translate("sortedDescending")} as={TiArrowSortedDown} boxSize={3} />
                  );
                } else if (sort === "asc") {
                  rightIcon = (
                    <Icon aria-label={translate("sortedAscending")} as={TiArrowSortedUp} boxSize={3} />
                  );
                } else {
                  rightIcon = (
                    <Icon aria-label={translate("sortedUnsorted")} as={TiArrowUnsorted} boxSize={3} />
                  );
                }

                return (
                  <Table.ColumnHeader colSpan={colSpan} key={id} paddingBlock={1} whiteSpace="nowrap">
                    {isPlaceholder ? undefined : (
                      <Button
                        _focus={{ color: "brand.500" }}
                        _hover={{ color: "brand.500" }}
                        aria-label={translate("sort")}
                        border={0}
                        color={sort === false ? undefined : "brand.500"}
                        disabled={!canSort}
                        gap={1}
                        onClick={column.getToggleSortingHandler()}
                        p={0}
                        variant="plain"
                      >
                        {text}
                        {rightIcon}
                      </Button>
                    )}
                  </Table.ColumnHeader>
                );
              }

              return (
                <Table.ColumnHeader colSpan={colSpan} key={id} paddingBlock={1} whiteSpace="nowrap">
                  {isPlaceholder ? undefined : text}
                </Table.ColumnHeader>
              );
            })}
          </Table.Row>
        ))}
      </Table.Header>
      <Table.Body>
        {rows.length === 0 ? (
          <Table.Row data-testid="table-no-rows">
            <Table.Cell colSpan={table.getVisibleLeafColumns().length}>{noRowsMessage}</Table.Cell>
          </Table.Row>
        ) : (
          rows.map((row) => (
            <Table.Row key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <Table.Cell data-testid={`table-cell-${cell.column.id}`} key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </Table.Cell>
              ))}
            </Table.Row>
          ))
        )}
      </Table.Body>
    </Table.Root>
  );
};
