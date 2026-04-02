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
import { Badge, Box, Heading, Link, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

type DeadlineRow = { row: { original: DeadlineResponse } };

const createColumns = (translate: TFunction): Array<ColumnDef<DeadlineResponse>> => [
  {
    accessorKey: "dag_id",
    cell: ({ row: { original } }: DeadlineRow) => (
      <Link asChild color="fg.info">
        <RouterLink to={`/dags/${original.dag_id}`}>
          <TruncatedText text={original.dag_id} />
        </RouterLink>
      </Link>
    ),
    header: translate("common:dagId"),
  },
  {
    accessorKey: "dag_run_id",
    cell: ({ row: { original } }: DeadlineRow) => (
      <Link asChild color="fg.info">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.dag_run_id}`}>
          <TruncatedText text={original.dag_run_id} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:dagRunId"),
  },
  {
    accessorKey: "deadline_time",
    cell: ({ row: { original } }: DeadlineRow) => <Time datetime={original.deadline_time} />,
    header: translate("browse:deadlines.columns.deadlineTime"),
  },
  {
    accessorKey: "missed",
    cell: ({
      row: {
        original: { missed },
      },
    }) => (
      <Badge colorPalette={missed ? "red" : "blue"} size="sm" variant="solid">
        {missed
          ? translate("browse:deadlines.filters.statusOptions.missed")
          : translate("browse:deadlines.filters.statusOptions.pending")}
      </Badge>
    ),
    header: translate("browse:deadlines.columns.status"),
  },
  {
    accessorKey: "alert_name",
    cell: ({ row: { original } }) => original.alert_name ?? "",
    enableSorting: false,
    header: translate("browse:deadlines.columns.alertName"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row: { original } }: DeadlineRow) => <Time datetime={original.created_at} />,
    header: translate("common:table.createdAt"),
  },
];

const deadlinesFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.DAG_ID,
  SearchParamsKeys.MISSED,
];

export const Deadlines = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const [searchParams] = useSearchParams();

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(deadlinesFilterKeys);

  const columns = createColumns(translate);

  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["deadline_time"];

  const filteredDagId = searchParams.get(SearchParamsKeys.DAG_ID);
  const filteredMissed = searchParams.get(SearchParamsKeys.MISSED);

  const missedFilter = filteredMissed === "true" ? true : filteredMissed === "false" ? false : undefined;

  const { data, error, isFetching, isLoading } = useDeadlinesServiceGetDeadlines({
    dagId: filteredDagId !== null && filteredDagId !== "" ? filteredDagId : "~",
    dagRunId: "~",
    limit: pagination.pageSize,
    missed: missedFilter,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  return (
    <Box p={2}>
      <Heading>{translate("browse:deadlines.title")}</Heading>
      <VStack align="start" gap={4} paddingY="4px">
        <FilterBar
          configs={filterConfigs}
          initialValues={initialValues}
          onFiltersChange={handleFiltersChange}
        />
      </VStack>
      <DataTable
        columns={columns}
        data={data?.deadlines ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="browse:deadlines.deadline"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
