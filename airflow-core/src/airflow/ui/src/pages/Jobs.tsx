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
import { Box, Heading, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useJobServiceGetJobs } from "openapi/queries";
import type { JobResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

const createColumns = (translate: TFunction): Array<ColumnDef<JobResponse>> => [
  {
    accessorKey: "id",
    header: translate("jobs.columns.id"),
  },
  {
    accessorKey: "job_type",
    header: translate("jobs.columns.jobType"),
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) =>
      // Job states (running, success, failed, restarting) are a subset of TaskInstanceState
      state === null ? undefined : (
        <StateBadge state={state as TaskInstanceState}>{translate(`common:states.${state}`)}</StateBadge>
      ),
    header: translate("common:state"),
  },
  {
    accessorKey: "hostname",
    header: translate("jobs.columns.hostname"),
  },
  {
    accessorKey: "start_date",
    cell: ({ row: { original } }) => <Time datetime={original.start_date} />,
    header: translate("common:startDate"),
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: translate("common:endDate"),
  },
  {
    accessorKey: "latest_heartbeat",
    cell: ({ row: { original } }) => <Time datetime={original.latest_heartbeat} />,
    header: translate("jobs.columns.latestHeartbeat"),
  },
  {
    accessorKey: "executor_class",
    header: translate("jobs.columns.executorClass"),
  },
  {
    accessorKey: "unixname",
    header: translate("jobs.columns.unixname"),
  },
];

const jobsFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.JOB_STATE,
  SearchParamsKeys.JOB_TYPE,
  SearchParamsKeys.HOSTNAME,
  SearchParamsKeys.EXECUTOR_CLASS,
  SearchParamsKeys.START_DATE_RANGE,
  SearchParamsKeys.END_DATE_RANGE,
];

export const Jobs = () => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const [searchParams] = useSearchParams();

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(jobsFilterKeys);

  const columns = createColumns(translate);

  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;

  const filteredJobState = searchParams.get(SearchParamsKeys.JOB_STATE);
  const filteredJobType = searchParams.get(SearchParamsKeys.JOB_TYPE);
  const filteredHostname = searchParams.get(SearchParamsKeys.HOSTNAME);
  const filteredExecutorClass = searchParams.get(SearchParamsKeys.EXECUTOR_CLASS);
  const filteredStartDateGte = searchParams.get(SearchParamsKeys.START_DATE_GTE);
  const filteredStartDateLte = searchParams.get(SearchParamsKeys.START_DATE_LTE);
  const filteredEndDateGte = searchParams.get(SearchParamsKeys.END_DATE_GTE);
  const filteredEndDateLte = searchParams.get(SearchParamsKeys.END_DATE_LTE);

  const { data, error, isFetching, isLoading } = useJobServiceGetJobs({
    endDateGte: filteredEndDateGte ?? undefined,
    endDateLte: filteredEndDateLte ?? undefined,
    executorClass: filteredExecutorClass ?? undefined,
    hostname: filteredHostname ?? undefined,
    jobState: filteredJobState ?? undefined,
    jobType: filteredJobType ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    startDateGte: filteredStartDateGte ?? undefined,
    startDateLte: filteredStartDateLte ?? undefined,
  });

  return (
    <Box p={2}>
      <Heading>{translate("common:browse.jobs")}</Heading>
      <VStack align="start" gap={4} paddingY="4px">
        <FilterBar
          configs={filterConfigs}
          initialValues={initialValues}
          onFiltersChange={handleFiltersChange}
        />
      </VStack>
      <DataTable
        columns={columns}
        data={data?.jobs ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:browse.jobs"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
