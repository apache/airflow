/* eslint-disable max-lines */

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
import { Flex, HStack, Link, type SelectValueChangeDetails, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback } from "react";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse, DagRunState, DagRunType } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { dagRunTypeOptions, dagRunStateOptions as stateOptions } from "src/constants/stateOptions";
import { capitalize, getDuration, useAutoRefresh, isStatePending } from "src/utils";

type DagRunRow = { row: { original: DAGRunResponse } };
const {
  END_DATE: END_DATE_PARAM,
  RUN_TYPE: RUN_TYPE_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const runColumns = (dagId?: string): Array<ColumnDef<DAGRunResponse>> => [
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_id",
          enableSorting: false,
          header: "Dag ID",
        },
      ]),
  {
    accessorKey: "run_after",
    cell: ({ row: { original } }: DagRunRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.dag_run_id}`}>
          <Time datetime={original.run_after} />
        </RouterLink>
      </Link>
    ),
    header: "Run After",
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => <StateBadge state={state}>{state}</StateBadge>,
    header: () => "State",
  },
  {
    accessorKey: "run_type",
    cell: ({ row: { original } }) => (
      <HStack>
        <RunTypeIcon runType={original.run_type} />
        <Text>{original.run_type}</Text>
      </HStack>
    ),
    enableSorting: false,
    header: "Run Type",
  },
  {
    accessorKey: "start_date",
    cell: ({ row: { original } }) => <Time datetime={original.start_date} />,
    header: "Start Date",
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: "End Date",
  },
  {
    cell: ({ row: { original } }) => getDuration(original.start_date, original.end_date),
    header: "Duration",
  },
  {
    accessorKey: "dag_versions",
    cell: ({ row: { original } }) => (
      <LimitedItemsList
        items={original.dag_versions.map((version) => (
          <DagVersion key={version.id} version={version} />
        ))}
        maxItems={4}
      />
    ),
    enableSorting: false,
    header: "Dag Version(s)",
  },
  {
    accessorKey: "actions",
    cell: ({ row }) => (
      <Flex justifyContent="end">
        <ClearRunButton dagRun={row.original} withText={false} />
        <MarkRunAsButton dagRun={row.original} withText={false} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const DagRuns = () => {
  const { dagId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-run_after";

  const filteredState = searchParams.get(STATE_PARAM);
  const filteredType = searchParams.get(RUN_TYPE_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useDagRunServiceGetDagRuns(
    {
      dagId: dagId ?? "~",
      endDateLte: endDate ?? undefined,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      runType: filteredType === null ? undefined : [filteredType],
      startDateGte: startDate ?? undefined,
      state: filteredState === null ? undefined : [filteredState],
    },
    undefined,
    {
      enabled: !isNaN(pagination.pageSize),
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((run) => isStatePending(run.state)) ? refetchInterval : false,
    },
  );

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.set(STATE_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleTypeChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(RUN_TYPE_PARAM);
      } else {
        searchParams.set(RUN_TYPE_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  return (
    <>
      <Flex>
        <Select.Root
          collection={stateOptions}
          maxW="200px"
          onValueChange={handleStateChange}
          value={[filteredState ?? "all"]}
        >
          <Select.Trigger colorPalette="blue" isActive={Boolean(filteredState)} minW="max-content">
            <Select.ValueText width="auto">
              {() =>
                filteredState === null ? (
                  "All States"
                ) : (
                  <StateBadge state={filteredState as DagRunState}>{capitalize(filteredState)}</StateBadge>
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {stateOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  option.label
                ) : (
                  <StateBadge state={option.value as DagRunState}>{option.label}</StateBadge>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
        <Select.Root
          collection={dagRunTypeOptions}
          maxW="200px"
          onValueChange={handleTypeChange}
          value={[filteredType ?? "all"]}
        >
          <Select.Trigger colorPalette="blue" isActive={Boolean(filteredState)} minW="max-content">
            <Select.ValueText width="auto">
              {() =>
                filteredType === null ? (
                  "All Run Types"
                ) : (
                  <Flex alignItems="center" gap={1}>
                    <RunTypeIcon runType={filteredType as DagRunType} />
                    {filteredType}
                  </Flex>
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {dagRunTypeOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  option.label
                ) : (
                  <Flex gap={1}>
                    <RunTypeIcon runType={option.value as DagRunType} />
                    {option.label}
                  </Flex>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Flex>
      <DataTable
        columns={runColumns(dagId)}
        data={data?.dag_runs ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="Dag Run"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </>
  );
};
