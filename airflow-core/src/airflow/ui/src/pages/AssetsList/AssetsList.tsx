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
import { VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useAssetServiceGetAssetsUi } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { AliasesPopover, WatchersPopover } from "src/components/Assets/ListPopover";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import { SearchBar } from "src/components/SearchBar";
import Time from "src/components/Time";
import { RouterLink } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAdvancedSearch, useAdvancedSearchArg } from "src/hooks/useAdvancedSearch";
import { CreateAssetEvent } from "src/pages/Asset/CreateAssetEvent";
import { useDocumentTitle, useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import { DependencyPopover } from "./DependencyPopover";

const assetsFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.GROUP_PATTERN,
  SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_RANGE,
];

type AssetRow = { row: { original: AssetResponse } };

const createColumns = (translate: TFunction): Array<ColumnDef<AssetResponse>> => [
  {
    accessorKey: "name",
    cell: ({ row: { original } }: AssetRow) => (
      <RouterLink fontWeight="bold" to={`/assets/${original.id}`}>
        {original.name}
      </RouterLink>
    ),
    header: () => translate("name"),
  },
  {
    accessorKey: "last_asset_event_timestamp",
    cell: ({ row: { original } }: AssetRow) => {
      const assetEvent = original.last_asset_event;
      const timestamp = assetEvent?.timestamp;

      if (timestamp === null || timestamp === undefined) {
        return undefined;
      }

      return <Time datetime={timestamp} />;
    },
    header: () => translate("lastAssetEvent"),
  },
  {
    accessorKey: "group",
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
    accessorKey: "consuming_tasks",
    cell: ({ row: { original } }: AssetRow) =>
      original.consuming_tasks.length ? (
        <DependencyPopover dependencies={original.consuming_tasks} type="Task" />
      ) : undefined,
    enableSorting: false,
    header: () => translate("consumingTasks"),
  },
  {
    accessorKey: "aliases",
    cell: ({ row: { original } }: AssetRow) =>
      original.aliases.length ? <AliasesPopover aliases={original.aliases} /> : undefined,
    enableSorting: false,
    header: () => translate("aliases"),
  },
  {
    accessorKey: "watchers",
    cell: ({ row: { original } }: AssetRow) =>
      original.watchers.length ? <WatchersPopover watchers={original.watchers} /> : undefined,
    enableSorting: false,
    header: () => translate("watchers"),
  },
  {
    accessorKey: "trigger",
    cell: ({ row }) => <CreateAssetEvent asset={row.original} />,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
];

const { NAME_PATTERN, OFFSET }: SearchParamsKeysType = SearchParamsKeys;

export const AssetsList = () => {
  const { t: translate } = useTranslation(["assets", "common"]);

  useDocumentTitle(translate("common:nav.assets"));

  const [searchParams, setSearchParams] = useSearchParams();

  const namePattern = searchParams.get(NAME_PATTERN) ?? "";
  const advancedSearch = useAdvancedSearch("assets");

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-last_asset_event_timestamp"];

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(assetsFilterKeys);

  const lastAssetEventTimestampGte = searchParams.get(SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_GTE);
  const lastAssetEventTimestampLte = searchParams.get(SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_LTE);
  const groupArg = useAdvancedSearchArg({
    patternApiKey: "groupPattern",
    prefixApiKey: "groupPrefixPattern",
    storageKey: SearchParamsKeys.GROUP_PATTERN,
    value: searchParams.get(SearchParamsKeys.GROUP_PATTERN),
  });

  const { data, error, isFetching, isLoading } = useAssetServiceGetAssetsUi(
    {
      ...groupArg,
      lastAssetEventTimestampGte: lastAssetEventTimestampGte ?? undefined,
      lastAssetEventTimestampLte: lastAssetEventTimestampLte ?? undefined,
      limit: pagination.pageSize,
      ...(advancedSearch.enabled ? { namePattern } : { namePrefixPattern: namePattern }),
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    { placeholderData: (prev) => prev },
  );

  const columns = createColumns(translate);
  const totalEntries = data?.total_entries ?? 0;

  const handleSearchChange = (value: string) => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    if (value) {
      searchParams.set(NAME_PATTERN, value);
    } else {
      searchParams.delete(NAME_PATTERN);
    }
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  return (
    <DataTable
      columns={columns}
      data={data?.assets ?? []}
      errorMessage={<ErrorAlert error={error} />}
      filterActions={
        <VStack alignItems="flex-start" gap={2} w="100%">
          <SearchBar
            advancedSearch={advancedSearch}
            defaultValue={namePattern}
            onChange={handleSearchChange}
            placeholder={translate("searchPlaceholder")}
          />
          <FilterBar
            configs={filterConfigs}
            initialValues={initialValues}
            onFiltersChange={handleFiltersChange}
          />
        </VStack>
      }
      initialState={tableURLState}
      isFetching={isFetching}
      isLoading={isLoading}
      modelName="common:asset"
      onStateChange={setTableURLState}
      total={totalEntries}
    />
  );
};
