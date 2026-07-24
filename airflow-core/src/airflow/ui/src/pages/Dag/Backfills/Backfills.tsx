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
import { Box, Heading, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useBackfillServiceListBackfillsUi } from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getDuration } from "src/utils";

import { BackfillsFilters } from "./BackfillsFilters";

const {
  COMPLETED_AT_GTE: COMPLETED_AT_GTE_PARAM,
  COMPLETED_AT_LTE: COMPLETED_AT_LTE_PARAM,
  CREATED_AT_GTE: CREATED_AT_GTE_PARAM,
  CREATED_AT_LTE: CREATED_AT_LTE_PARAM,
  END_DATE_GTE: END_DATE_GTE_PARAM,
  END_DATE_LTE: END_DATE_LTE_PARAM,
  MAX_ACTIVE_RUNS_GTE: MAX_ACTIVE_RUNS_GTE_PARAM,
  MAX_ACTIVE_RUNS_LTE: MAX_ACTIVE_RUNS_LTE_PARAM,
  REPROCESS_BEHAVIOR: REPROCESS_BEHAVIOR_PARAM,
  START_DATE_GTE: START_DATE_GTE_PARAM,
  START_DATE_LTE: START_DATE_LTE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const getColumns = (translate: (key: string) => string): Array<ColumnDef<BackfillResponse>> => [
  {
    accessorKey: "date_from",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.from_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.from"),
  },
  {
    accessorKey: "date_to",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.to_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.to"),
  },
  {
    accessorKey: "reprocess_behavior",
    cell: ({ row }) => (
      <Text>
        {row.original.reprocess_behavior === "none"
          ? translate("components:backfill.missingRuns")
          : row.original.reprocess_behavior === "failed"
            ? translate("components:backfill.missingAndErroredRuns")
            : translate("components:backfill.allRuns")}
      </Text>
    ),
    enableSorting: false,
    header: translate("components:backfill.reprocessBehavior"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.created_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.createdAt"),
  },
  {
    accessorKey: "completed_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.completed_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.completedAt"),
  },
  {
    accessorKey: "duration",
    cell: ({ row }) => (
      <Text>
        {row.original.completed_at === null
          ? ""
          : getDuration(row.original.created_at, row.original.completed_at)}
      </Text>
    ),
    enableSorting: false,
    header: translate("duration"),
  },
  {
    accessorKey: "max_active_runs",
    enableSorting: false,
    header: translate("table.maxActiveRuns"),
  },
];

export const Backfills = () => {
  const { t: translate } = useTranslation();
  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination } = tableURLState;

  const { dagId = "" } = useParams();
  const [searchParams] = useSearchParams();

  const startDateGte = searchParams.get(START_DATE_GTE_PARAM);
  const startDateLte = searchParams.get(START_DATE_LTE_PARAM);
  const endDateGte = searchParams.get(END_DATE_GTE_PARAM);
  const endDateLte = searchParams.get(END_DATE_LTE_PARAM);
  const createdAtGte = searchParams.get(CREATED_AT_GTE_PARAM);
  const createdAtLte = searchParams.get(CREATED_AT_LTE_PARAM);
  const completedAtGte = searchParams.get(COMPLETED_AT_GTE_PARAM);
  const completedAtLte = searchParams.get(COMPLETED_AT_LTE_PARAM);
  const maxActiveRunsGte = searchParams.get(MAX_ACTIVE_RUNS_GTE_PARAM);
  const maxActiveRunsLte = searchParams.get(MAX_ACTIVE_RUNS_LTE_PARAM);
  const reprocessBehavior = searchParams.get(REPROCESS_BEHAVIOR_PARAM);

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillsUi({
    completedAtGte: completedAtGte ?? undefined,
    completedAtLte: completedAtLte ?? undefined,
    createdAtGte: createdAtGte ?? undefined,
    createdAtLte: createdAtLte ?? undefined,
    dagId,
    fromDateGte: startDateGte ?? undefined,
    fromDateLte: startDateLte ?? undefined,
    limit: pagination.pageSize,
    maxActiveRunsGte:
      maxActiveRunsGte !== null && maxActiveRunsGte !== "" ? Number(maxActiveRunsGte) : undefined,
    maxActiveRunsLte:
      maxActiveRunsLte !== null && maxActiveRunsLte !== "" ? Number(maxActiveRunsLte) : undefined,
    offset: pagination.pageIndex * pagination.pageSize,
    reprocessBehavior: reprocessBehavior ?? undefined,
    toDateGte: endDateGte ?? undefined,
    toDateLte: endDateLte ?? undefined,
  });

  const columns = getColumns(translate);

  return (
    <Box>
      <BackfillsFilters />
      <ErrorAlert error={error} />
      <Heading my={1} size="md">
        {translate("backfill", { count: data ? data.total_entries : 0 })}
      </Heading>
      <DataTable
        columns={columns}
        data={data ? data.backfills : []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:backfill"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
