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
import { Badge, Box, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useOutletContext, useParams } from "react-router-dom";

import { useBackfillServiceListBackfillDagRuns } from "openapi/queries";
import type { BackfillDagRunResponse, BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { RouterLink } from "src/components/ui";
import { useAutoRefresh } from "src/utils";

const translateExceptionReason = (reason: string, translate: (key: string) => string) => {
  // Keep this mapping in sync with BackfillDagRunExceptionReason in airflow.models.backfill.
  switch (reason) {
    case "already exists":
      return translate("components:backfill.exceptionReason.alreadyExists");
    case "in flight":
      return translate("components:backfill.exceptionReason.inFlight");
    default:
      return translate("components:backfill.exceptionReason.unknown");
  }
};

const getColumns = (translate: (key: string) => string): Array<ColumnDef<BackfillDagRunResponse>> => [
  {
    accessorKey: "logical_date",
    cell: ({ row }) => {
      if (row.original.partition_key !== null && row.original.partition_key !== "") {
        return <Text>{row.original.partition_key}</Text>;
      }

      if (row.original.logical_date !== null && row.original.logical_date !== "") {
        return (
          <Text>
            <Time datetime={row.original.logical_date} />
          </Text>
        );
      }

      return <Text color="fg.muted">—</Text>;
    },
    enableSorting: false,
    header: translate("slot"),
  },
  {
    accessorKey: "dag_run_state",
    cell: ({ row }) => {
      if (row.original.exception_reason !== null && row.original.exception_reason !== "") {
        return (
          <Badge colorPalette="orange" variant="subtle">
            {translateExceptionReason(row.original.exception_reason, translate)}
          </Badge>
        );
      }

      const state = row.original.dag_run_state;

      return (
        <StateBadge state={state ?? null}>
          {state === null || state === undefined ? "—" : translate(`states.${state}`)}
        </StateBadge>
      );
    },
    enableSorting: false,
    header: translate("state"),
  },
  {
    accessorKey: "dag_run_id",
    cell: ({ row }) => {
      const runId = row.original.dag_run_id;

      if (runId === null || runId === undefined || runId === "") {
        return <Text color="fg.muted">—</Text>;
      }

      return (
        <RouterLink fontWeight="bold" to={`/dags/${row.original.dag_id}/runs/${runId}`}>
          {runId}
        </RouterLink>
      );
    },
    enableSorting: false,
    header: translate("runId"),
  },
  {
    accessorKey: "sort_ordinal",
    enableSorting: false,
    header: "#",
  },
];

export const BackfillDagRuns = () => {
  const { t: translate } = useTranslation();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;
  const { backfillId = "", dagId = "" } = useParams();
  const backfill = useOutletContext<BackfillResponse | undefined>();
  const parsedBackfillId = Number(backfillId);
  const hasValidBackfillId = Number.isInteger(parsedBackfillId) && parsedBackfillId > 0;
  const refetchInterval = useAutoRefresh({ dagId });
  const shouldPoll = backfill?.completed_at === null && !backfill.is_paused;

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillDagRuns(
    {
      backfillId: hasValidBackfillId ? parsedBackfillId : 0,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
    },
    undefined,
    {
      enabled: hasValidBackfillId,
      refetchInterval: shouldPoll ? refetchInterval : false,
    },
  );

  const columns = getColumns(translate);

  return (
    <Box>
      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.backfill_dag_runs ?? []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:dagRun"
        onStateChange={setTableURLState}
        total={data?.total_entries ?? 0}
      />
    </Box>
  );
};
