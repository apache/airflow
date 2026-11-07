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
import { Box, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTriggerServiceGetTriggers } from "openapi/queries";
import type { TriggerDetailResponse, TriggerKind, TriggerStatus } from "openapi/requests/types.gen";

import { RouterLink, Tooltip } from "src/system-components";

import { ListPopover } from "src/components/Assets/ListPopover";
import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";
import { TriggerStatusBadge } from "src/components/TriggerStatusBadge";
import { TruncatedText } from "src/components/TruncatedText";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import {
  type DurationFormat,
  getErrorStatus,
  useAutoRefresh,
  useDocumentTitle,
  useDurationFormat,
  useFiltersHandler,
  type FilterableSearchParamsKeys,
} from "src/utils";

import { TriggerOwner } from "./TriggerOwner";
import { TriggersForbidden } from "./TriggersForbidden";
import { getScheduledDagIds } from "./getScheduledDagIds";
import { getTriggerClassName } from "./getTriggerClassName";

type TriggerRow = { row: { original: TriggerDetailResponse } };

const createColumns = (
  translate: TFunction,
  formatRelative: DurationFormat["formatRelative"],
  multiTeam: boolean,
): Array<ColumnDef<DataTableFeatures, TriggerDetailResponse>> => [
  {
    accessorKey: "id",
    cell: ({ row: { original } }: TriggerRow) => (
      <RouterLink fontWeight="bold" to={`/triggers/${original.id}`}>
        {original.id}
      </RouterLink>
    ),
    header: translate("common:taskInstance.triggerer.id"),
  },
  {
    accessorKey: "classpath",
    cell: ({ row: { original } }: TriggerRow) => (
      <Tooltip content={original.classpath}>
        <Text as="span">{getTriggerClassName(original.classpath)}</Text>
      </Tooltip>
    ),
    enableSorting: false,
    header: translate("common:taskInstance.triggerer.class"),
  },
  {
    accessorKey: "kind",
    cell: ({ row: { original } }: TriggerRow) => (
      <Text whiteSpace="nowrap">{translate(`triggers.kind.${original.kind}`)}</Text>
    ),
    enableSorting: false,
    header: translate("triggers.columns.kind"),
  },
  {
    accessorKey: "status",
    cell: ({ row: { original } }: TriggerRow) => <TriggerStatusBadge status={original.status} />,
    enableSorting: false,
    header: translate("triggers.columns.status"),
  },
  {
    cell: ({ row: { original } }: TriggerRow) => <TriggerOwner trigger={original} />,
    enableSorting: false,
    header: translate("triggers.columns.owner"),
    id: "owner",
  },
  {
    cell: ({ row: { original } }: TriggerRow) => {
      if (original.kind !== "asset_watcher") {
        return "-";
      }
      const dagIds = getScheduledDagIds(original.asset_watchers);

      return dagIds.length === 0 ? (
        0
      ) : (
        <ListPopover
          items={dagIds.map((dagId) => ({
            key: dagId,
            label: <RouterLink to={`/dags/${dagId}`}>{dagId}</RouterLink>,
          }))}
          noun={translate("common:dag", { count: dagIds.length })}
        />
      );
    },
    enableSorting: false,
    header: translate("triggers.columns.dagsAffected"),
    id: "dags_affected",
  },
  ...(multiTeam
    ? [
        {
          accessorKey: "team_name",
          cell: ({ row: { original } }: TriggerRow) => <TeamName teamName={original.team_name} />,
          enableSorting: false,
          header: translate("common:dagDetails.team"),
        },
      ]
    : []),
  {
    accessorKey: "triggerer_hostname",
    // Host names break at every hyphen otherwise, three lines for a typical pod name.
    cell: ({ row: { original } }: TriggerRow) => (
      <Text whiteSpace="nowrap">{original.triggerer_hostname}</Text>
    ),
    enableSorting: false,
    header: translate("triggers.columns.triggerer"),
  },
  {
    accessorKey: "consecutive_failures",
    header: translate("triggers.columns.failures"),
  },
  {
    accessorKey: "last_error",
    cell: ({ row: { original } }: TriggerRow) =>
      original.last_error === null ? undefined : (
        // Kept after a recovery, so it is only an error colour until the trigger recovers, and says when.
        <Box>
          <TruncatedText
            color={original.consecutive_failures > 0 ? "fg.error" : "fg.muted"}
            text={original.last_error}
          />
          <Text color="fg.muted" data-testid="trigger-last-failed" fontSize="xs">
            {formatRelative(original.last_failed_at)}
          </Text>
        </Box>
      ),
    enableSorting: false,
    header: translate("triggers.columns.lastError"),
  },
  {
    accessorKey: "last_event_at",
    cell: ({ row: { original } }: TriggerRow) => <Time datetime={original.last_event_at} />,
    header: translate("triggers.columns.lastEvent"),
  },
  {
    accessorKey: "created_date",
    cell: ({ row: { original } }: TriggerRow) => <Time datetime={original.created_date} />,
    header: translate("common:table.createdAt"),
  },
];

const triggersFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.TRIGGER_KIND,
  SearchParamsKeys.TRIGGER_STATUS,
  SearchParamsKeys.TRIGGERER_HOSTNAME,
];

export const Triggers = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  useDocumentTitle(translate("common:browse.triggers"));

  const { setTableURLState, tableURLState } = useTableURLState();
  const refetchInterval = useAutoRefresh({});
  const [searchParams] = useSearchParams();
  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(triggersFilterKeys);

  const { formatRelative } = useDurationFormat();
  const columns = createColumns(translate, formatRelative, multiTeamEnabled);

  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-id"];

  const kinds = searchParams.getAll(SearchParamsKeys.TRIGGER_KIND) as Array<TriggerKind>;
  const statuses = searchParams.getAll(SearchParamsKeys.TRIGGER_STATUS) as Array<TriggerStatus>;
  const hostname = searchParams.get(SearchParamsKeys.TRIGGERER_HOSTNAME);
  const { data, error, isLoading } = useTriggerServiceGetTriggers(
    {
      kind: kinds.length > 0 ? kinds : undefined,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      status: statuses.length > 0 ? statuses : undefined,
      triggererHostname: hostname ?? undefined,
    },
    undefined,
    {
      refetchInterval: ({ state }) => state.status !== "error" && refetchInterval,
    },
  );

  return (
    <Box px={2}>
      <DataTable
        columns={columns}
        data={data?.triggers ?? []}
        errorMessage={getErrorStatus(error) === 403 ? <TriggersForbidden /> : <ErrorAlert error={error} />}
        filterActions={
          <FilterBar
            configs={filterConfigs}
            initialValues={initialValues}
            onFiltersChange={handleFiltersChange}
          />
        }
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="common:trigger"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
