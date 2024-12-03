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
import {
  Box,
  createListCollection,
  Flex,
  HStack,
  Link,
  type SelectValueChangeDetails,
  Text,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import dayjs from "dayjs";
import { useCallback } from "react";
import {
  useParams,
  Link as RouterLink,
  useSearchParams,
} from "react-router-dom";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse, DagRunState } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { Select, Status } from "src/components/ui";
import { capitalize } from "src/utils";

const columns: Array<ColumnDef<DAGRunResponse>> = [
  {
    accessorKey: "run_id",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.dag_run_id}`}>
          {original.dag_run_id}
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: "Run ID",
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => <Status state={state}>{state}</Status>,
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
    cell: ({ row: { original } }) =>
      `${dayjs.duration(dayjs(original.end_date).diff(original.start_date)).asSeconds().toFixed(2)}s`,
    header: "Duration",
  },
  {
    accessorKey: "note",
    enableSorting: false,
    header: "Note",
  },
];

const stateOptions = createListCollection({
  items: [
    { label: "All States", value: "all" },
    { label: "Queued", value: "queued" },
    { label: "Running", value: "running" },
    { label: "Failed", value: "failed" },
    { label: "Success", value: "success" },
  ],
});

const STATE_PARAM = "state";

export const Runs = () => {
  const { dagId } = useParams();

  const [searchParams, setSearchParams] = useSearchParams();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const filteredState = searchParams.get(STATE_PARAM);

  const { data, error, isFetching, isLoading } = useDagRunServiceGetDagRuns({
    dagId: dagId ?? "~",
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    state: filteredState === null ? undefined : [filteredState],
  });

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.set(STATE_PARAM, val);
      }
      setSearchParams(searchParams);
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  return (
    <Box pt={4}>
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
                  <Status state={filteredState as DagRunState}>
                    {capitalize(filteredState)}
                  </Status>
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
                  <Status state={option.value as DagRunState}>
                    {option.label}
                  </Status>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Flex>
      <DataTable
        columns={columns}
        data={data?.dag_runs ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Dag Run"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
