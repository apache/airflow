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
import type { DAGRunResponse, DagRunState } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { MarkRunAsButton } from "src/components/MarkAs";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { dagRunStateOptions as stateOptions } from "src/constants/stateOptions";
import { capitalize, getDuration, useAutoRefresh, isStatePending } from "src/utils";

type DagRunRow = { row: { original: DAGRunResponse } };

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

const STATE_PARAM = "state";

export const DagRuns = () => {
  const { dagId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-run_after";

  const filteredState = searchParams.get(STATE_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useDagRunServiceGetDagRuns(
    {
      dagId: dagId ?? "~",
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
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

  return (
    <>
      <Flex>
        <Select.Root
          collection={stateOptions}
          maxW="200px"
          onValueChange={handleStateChange}
          value={[filteredState ?? "all"]}
        >
          <Select.Trigger colorPalette="blue" isActive={Boolean(filteredState)}>
            <Select.ValueText width={20}>
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
