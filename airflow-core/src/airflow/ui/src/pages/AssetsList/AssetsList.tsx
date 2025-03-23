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
import { Box, Heading, Link, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useState } from "react";
import { useSearchParams, Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetAssets } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { CreateAssetEvent } from "src/pages/Asset/CreateAssetEvent";
import { pluralize } from "src/utils";

import { DependencyPopover } from "./DependencyPopover";

type AssetRow = { row: { original: AssetResponse } };

const columns: Array<ColumnDef<AssetResponse>> = [
  {
    accessorKey: "name",
    cell: ({ row: { original } }: AssetRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/assets/${original.id}`}>{original.name}</RouterLink>
      </Link>
    ),
    header: () => "Name",
  },
  {
    accessorKey: "group",
    enableSorting: false,
    header: () => "Group",
  },
  {
    accessorKey: "consuming_dags",
    cell: ({ row: { original } }: AssetRow) =>
      original.consuming_dags.length ? (
        <DependencyPopover dependencies={original.consuming_dags} type="Dag" />
      ) : undefined,
    enableSorting: false,
    header: () => "Consuming Dags",
  },
  {
    accessorKey: "producing_tasks",
    cell: ({ row: { original } }: AssetRow) =>
      original.producing_tasks.length ? (
        <DependencyPopover dependencies={original.producing_tasks} type="Task" />
      ) : undefined,
    enableSorting: false,
    header: () => "Producing Tasks",
  },
  {
    accessorKey: "trigger",
    cell: ({ row }) => <CreateAssetEvent asset={row.original} withText={false} />,
    enableSorting: false,
    header: "",
  },
];

const NAME_PATTERN_PARAM = SearchParamsKeys.NAME_PATTERN;

export const AssetsList = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [namePattern, setNamePattern] = useState(searchParams.get(NAME_PATTERN_PARAM) ?? undefined);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const { data, error, isLoading } = useAssetServiceGetAssets({
    limit: pagination.pageSize,
    namePattern,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setSearchParams(searchParams);
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setNamePattern(value);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={namePattern ?? ""}
          onChange={handleSearchChange}
          placeHolder="Search Assets"
        />

        <Heading py={3} size="md">
          {pluralize("Asset", data?.total_entries)}
        </Heading>
      </VStack>
      <Box overflow="auto">
        <DataTable
          columns={columns}
          data={data?.assets ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName="Asset"
          onStateChange={setTableURLState}
          total={data?.total_entries}
        />
      </Box>
    </>
  );
};
