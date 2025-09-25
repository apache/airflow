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
import { useTranslation } from "react-i18next";
import { useSearchParams, Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetAssets } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import Time from "src/components/Time";
import { SearchParamsKeys } from "src/constants/searchParams";
import { CreateAssetEvent } from "src/pages/Asset/CreateAssetEvent";

import { DependencyPopover } from "./DependencyPopover";

type AssetRow = { row: { original: AssetResponse } };

const createColumns = (translate: (key: string) => string): Array<ColumnDef<AssetResponse>> => [
  {
    accessorKey: "name",
    cell: ({ row: { original } }: AssetRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/assets/${original.id}`}>{original.name}</RouterLink>
      </Link>
    ),
    header: () => translate("name"),
  },
  {
    accessorKey: "last_asset_event",
    cell: ({ row: { original } }: AssetRow) => {
      const assetEvent = original.last_asset_event;
      const timestamp = assetEvent?.timestamp;

      if (timestamp === null || timestamp === undefined) {
        return undefined;
      }

      return <Time datetime={timestamp} />;
    },
    enableSorting: false,
    header: () => translate("lastAssetEvent"),
  },
  {
    accessorKey: "group",
    enableSorting: false,
    header: () => translate("group"),
  },
  {
    accessorKey: "scheduled_dags",
    cell: ({ row: { original } }: AssetRow) =>
      original.scheduled_dags.length ? (
        <DependencyPopover dependencies={original.scheduled_dags} type="Dag" />
      ) : undefined,
    enableSorting: false,
    header: () => translate("scheduledDags"),
  },
  {
    accessorKey: "producing_tasks",
    cell: ({ row: { original } }: AssetRow) =>
      original.producing_tasks.length ? (
        <DependencyPopover dependencies={original.producing_tasks} type="Task" />
      ) : undefined,
    enableSorting: false,
    header: () => translate("producingTasks"),
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
  const { t: translate } = useTranslation(["assets", "common"]);
  const [searchParams, setSearchParams] = useSearchParams();

  const namePattern = searchParams.get(NAME_PATTERN_PARAM) ?? "";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;

  const { data, error, isLoading } = useAssetServiceGetAssets({
    limit: pagination.pageSize,
    namePattern,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const handleSearchChange = (value: string) => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setSearchParams(searchParams);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={namePattern}
          onChange={handleSearchChange}
          placeHolder={translate("searchPlaceholder")}
        />

        <Heading py={3} size="md">
          {data?.total_entries} {translate("common:asset", { count: data?.total_entries })}
        </Heading>
      </VStack>
      <Box overflow="auto">
        <DataTable
          columns={createColumns(translate)}
          data={data?.assets ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName={translate("common:asset_one")}
          onStateChange={setTableURLState}
          total={data?.total_entries}
        />
      </Box>
    </>
  );
};
