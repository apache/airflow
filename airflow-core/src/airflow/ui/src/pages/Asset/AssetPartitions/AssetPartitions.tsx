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
import { Box, Button, HStack, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useAssetServiceGetAssetPartitions } from "openapi/queries";
import type { AssetPartitionResponse } from "openapi/requests/types.gen";

import { RouterLink, Tooltip } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { formatNumber } from "src/utils";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils/useFiltersHandler";

import { PartitionDayGrid } from "./PartitionDayGrid";
import {
  groupByMonth,
  PARTITION_STATUSES,
  type PartitionStatus,
  RUN_STATE_BY_STATUS,
  STATUS_LABEL_KEY,
} from "./partitionStatus";

const FILTER_KEYS: Array<FilterableSearchParamsKeys> = [SearchParamsKeys.PARTITION_KEY];

const isPartitionStatus = (value: string): value is PartitionStatus =>
  (PARTITION_STATUSES as Array<string>).includes(value);

const getColumns = (
  translate: (key: string) => string,
  language: string,
): Array<ColumnDef<DataTableFeatures, AssetPartitionResponse>> => [
  {
    accessorKey: "partition_key",
    enableSorting: false,
    header: translate("common:dagRun.partitionKey"),
  },
  {
    accessorKey: "status",
    cell: ({ row: { original } }) => (
      <StateBadge state={RUN_STATE_BY_STATUS[original.status]}>
        {translate(STATUS_LABEL_KEY[original.status])}
      </StateBadge>
    ),
    enableSorting: false,
    header: translate("common:state"),
  },
  {
    accessorKey: "last_producer_run",
    cell: ({ row: { original } }) => {
      const run = original.last_producer_run;

      return run === null ? (
        <Text color="fg.muted">—</Text>
      ) : (
        <HStack gap={2}>
          <StateBadge state={run.state} />
          <RouterLink
            overflowWrap="anywhere"
            to={`/dags/${run.dag_id}/runs/${encodeURIComponent(run.run_id)}`}
          >
            {run.dag_id}
          </RouterLink>
        </HStack>
      );
    },
    enableSorting: false,
    header: translate("assets:partitions.lastProducerRun"),
  },
  {
    accessorKey: "last_event_timestamp",
    cell: ({ row: { original } }) =>
      original.last_event_timestamp === null ? (
        <Text color="fg.muted">—</Text>
      ) : (
        <Time datetime={original.last_event_timestamp} whiteSpace="nowrap" />
      ),
    enableSorting: false,
    header: translate("assets:lastAssetEvent"),
  },
  {
    accessorKey: "consumer_run_count",
    cell: ({ row: { original } }) => formatNumber(original.consumer_run_count ?? 0, language),
    enableSorting: false,
    header: () => (
      <Tooltip content={translate("assets:partitions.consumerRunsHelp")}>
        <span>{translate("assets:partitions.consumerRuns")}</span>
      </Tooltip>
    ),
  },
];

export const AssetPartitions = () => {
  const { i18n, t: translate } = useTranslation(["assets", "common"]);
  const { assetId: rawAssetId } = useParams();
  const assetId = rawAssetId === undefined ? 0 : parseInt(rawAssetId, 10);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;
  const [searchParams, setSearchParams] = useSearchParams();
  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(FILTER_KEYS);

  const selectedStatuses = searchParams.getAll(SearchParamsKeys.STATE).filter(isPartitionStatus);

  const { data, error, isLoading } = useAssetServiceGetAssetPartitions(
    {
      assetId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      partitionKeyPattern: searchParams.get(SearchParamsKeys.PARTITION_KEY) ?? undefined,
      status: selectedStatuses.length === 0 ? undefined : selectedStatuses,
    },
    undefined,
    { enabled: assetId !== 0 },
  );

  const toggleStatus = (status: PartitionStatus) => {
    const next = new URLSearchParams(searchParams);
    const remaining = selectedStatuses.filter((selected) => selected !== status);

    next.delete(SearchParamsKeys.STATE);
    (remaining.length === selectedStatuses.length ? [...selectedStatuses, status] : remaining).forEach(
      (selected) => next.append(SearchParamsKeys.STATE, selected),
    );
    // The filtered list is shorter, so the current page may not exist in it.
    next.delete(SearchParamsKeys.OFFSET);
    setSearchParams(next);
  };

  const partitions = data?.partitions ?? [];
  const months = groupByMonth(partitions);

  return (
    <Box display="flex" flex={1} flexDirection="column" gap={3} minH={0} overflow="auto" px={4} py={2}>
      <ErrorAlert error={error} />
      <HStack flexWrap="wrap" gap={1} ms={-2}>
        {PARTITION_STATUSES.map((status) => (
          <Button
            aria-pressed={selectedStatuses.includes(status)}
            data-testid={`partition-status-${status}`}
            key={status}
            onClick={() => toggleStatus(status)}
            px={2}
            size="sm"
            variant={selectedStatuses.includes(status) ? "subtle" : "ghost"}
          >
            <StateBadge fontSize="md" state={RUN_STATE_BY_STATUS[status]}>
              {data === undefined ? " " : formatNumber(data.status_counts[status] ?? 0, i18n.language)}
            </StateBadge>
            {translate(STATUS_LABEL_KEY[status])}
          </Button>
        ))}
      </HStack>
      {months === undefined ? undefined : <PartitionDayGrid months={months} />}
      <DataTable
        columns={getColumns(translate, i18n.language)}
        data={partitions}
        displayMode="table"
        filterActions={
          <FilterBar
            configs={filterConfigs}
            initialValues={initialValues}
            onFiltersChange={handleFiltersChange}
          />
        }
        hideRowCountHeading
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="common:partition"
        onStateChange={setTableURLState}
        total={data?.total_entries ?? 0}
      />
    </Box>
  );
};
