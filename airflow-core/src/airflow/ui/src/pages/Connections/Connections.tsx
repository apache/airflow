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
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useConnectionServiceGetConnections } from "openapi/queries";
import type { ConnectionResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConnectionTypeMeta } from "src/queries/useConnectionTypeMeta";

import AddConnectionButton from "./AddConnectionButton";
import DeleteConnectionButton from "./DeleteConnectionButton";
import EditConnectionButton from "./EditConnectionButton";

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

const columns: Array<ColumnDef<ConnectionResponse>> = [
  {
    accessorKey: "connection_id",
    header: "Connection Id",
  },
  {
    accessorKey: "conn_type",
    header: "Connection Type",
  },
  {
    accessorKey: "description",
    header: "Description",
  },
  {
    accessorKey: "host",
    header: "Host",
  },
  {
    accessorKey: "port",
    header: "Port",
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditConnectionButton connection={original} disabled={false} />
        <DeleteConnectionButton connectionId={original.connection_id} disabled={false} />
        {/* For now disabled is set as false, will depend on selected rows once multi action PR merges */}
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
  const { setTableURLState, tableURLState } = useTableURLState();
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [connectionIdPattern, setConnectionIdPattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  useConnectionTypeMeta(); // Pre-fetch connection type metadata
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-connection_id";
  const { data, error, isFetching, isLoading } = useConnectionServiceGetConnections({
    connectionIdPattern: connectionIdPattern ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

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
          placeHolder="Search Connections"
        />
        <HStack gap={4} mt={2}>
          <Spacer />
          <AddConnectionButton />
        </HStack>
      </VStack>

      <Box>
        <DataTable
          columns={columns}
          data={data?.connections ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isFetching={isFetching}
          isLoading={isLoading}
          modelName="Connection"
          onStateChange={setTableURLState}
          total={data?.total_entries}
        />
      </Box>
    </>
  );
};
