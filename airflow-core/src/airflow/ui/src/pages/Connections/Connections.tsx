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
import { Box, Flex, HStack, Spacer, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useConnectionServiceGetConnections } from "openapi/queries";
import type { ConnectionResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useRowSelection, type GetColumnsParams } from "src/components/DataTable/useRowSelection";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { Tooltip } from "src/components/ui";
import { ActionBar } from "src/components/ui/ActionBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";

import AddConnectionButton from "./AddConnectionButton";
import DeleteConnectionButton from "./DeleteConnectionButton";
import DeleteConnectionsButton from "./DeleteConnectionsButton";
import EditConnectionButton from "./EditConnectionButton";
import { NothingFoundInfo } from "./NothingFoundInfo";
import TestConnectionButton from "./TestConnectionButton";

export type ConnectionBody = {
  conn_type: string;
  connection_id: string;
  description: string;
  extra: string;
  host: string;
  login: string;
  password: string;
  port: string;
  schema: string;
};

const getColumns = ({
  allRowsSelected,
  onRowSelect,
  onSelectAll,
  selectedRows,
  translate,
}: { translate: TFunction } & GetColumnsParams): Array<ColumnDef<ConnectionResponse>> => [
  {
    accessorKey: "select",
    cell: ({ row }) => (
      <Checkbox
        borderWidth={1}
        checked={selectedRows.get(row.original.connection_id)}
        colorPalette="brand"
        onCheckedChange={(event) => onRowSelect(row.original.connection_id, Boolean(event.checked))}
      />
    ),
    enableHiding: false,
    enableSorting: false,
    header: () => (
      <Checkbox
        borderWidth={1}
        checked={allRowsSelected}
        colorPalette="brand"
        onCheckedChange={(event) => onSelectAll(Boolean(event.checked))}
      />
    ),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "connection_id",
    header: translate("connections.columns.connectionId"),
  },
  {
    accessorKey: "conn_type",
    header: translate("connections.columns.connectionType"),
  },
  {
    accessorKey: "description",
    header: translate("columns.description"),
  },
  {
    accessorKey: "host",
    header: translate("connections.columns.host"),
  },
  {
    accessorKey: "port",
    header: translate("connections.columns.port"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <TestConnectionButton connection={original} />
        <EditConnectionButton connection={original} disabled={selectedRows.size > 0} />
        <DeleteConnectionButton connectionId={original.connection_id} disabled={selectedRows.size > 0} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const Connections = () => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [connectionIdPattern, setConnectionIdPattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  useConnectionTypeMeta(); // Pre-fetch connection type metadata
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["connection_id"];
  const { data, error, isFetching, isLoading } = useConnectionServiceGetConnections({
    connectionIdPattern: connectionIdPattern ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const { allRowsSelected, clearSelections, handleRowSelect, handleSelectAll, selectedRows } =
    useRowSelection({
      data: data?.connections,
      getKey: (connection) => connection.connection_id,
    });

  const columns = useMemo(
    () =>
      getColumns({
        allRowsSelected,
        onRowSelect: handleRowSelect,
        onSelectAll: handleSelectAll,
        selectedRows,
        translate,
      }),
    [allRowsSelected, handleRowSelect, handleSelectAll, selectedRows, translate],
  );

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setSearchParams(searchParams);
    setConnectionIdPattern(value);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={connectionIdPattern ?? ""}
          onChange={handleSearchChange}
          placeHolder={translate("connections.searchPlaceholder")}
        />
        <HStack gap={4} mt={2}>
          <Spacer />
          <AddConnectionButton />
        </HStack>
      </VStack>

      <Box overflow="auto">
        <DataTable
          columns={columns}
          data={data?.connections ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isFetching={isFetching}
          isLoading={isLoading}
          modelName={translate("common:admin.Connections")}
          noRowsMessage={<NothingFoundInfo />}
          onStateChange={setTableURLState}
          total={data?.total_entries ?? 0}
        />
      </Box>
      <ActionBar.Root closeOnInteractOutside={false} open={Boolean(selectedRows.size)}>
        <ActionBar.Content>
          <ActionBar.SelectionTrigger>
            {selectedRows.size} {translate("deleteActions.selected")}
          </ActionBar.SelectionTrigger>
          <ActionBar.Separator />
          <Tooltip content={translate("deleteActions.tooltip")}>
            <DeleteConnectionsButton
              clearSelections={clearSelections}
              deleteKeys={[...selectedRows.keys()]}
            />
          </Tooltip>
          <ActionBar.CloseTrigger onClick={clearSelections} />
        </ActionBar.Content>
      </ActionBar.Root>
    </>
  );
};
