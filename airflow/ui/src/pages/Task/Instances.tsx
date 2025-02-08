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
import { Box, Flex, HStack, Link, type SelectValueChangeDetails } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback } from "react";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances, useTaskServiceGetTask } from "openapi/queries";
import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { taskInstanceStateOptions as stateOptions } from "src/constants/stateOptions";
import { capitalize, getDuration } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

const columns = (isMapped?: boolean): Array<ColumnDef<TaskInstanceResponse>> => [
  {
    accessorKey: "dag_run_id",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={getTaskInstanceLink(original)}>{original.dag_run_id}</RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: "Dag Run ID",
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
    accessorKey: "start_date",
    cell: ({ row: { original } }) => <Time datetime={original.start_date} />,
    header: "Start Date",
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: "End Date",
  },
  ...(isMapped
    ? [
        {
          accessorFn: (row: TaskInstanceResponse) => row.rendered_map_index ?? row.map_index,
          header: "Map Index",
        },
      ]
    : []),
  {
    accessorKey: "try_number",
    enableSorting: false,
    header: "Try Number",
  },
  {
    cell: ({ row: { original } }) => `${getDuration(original.start_date, original.end_date)}s`,
    header: "Duration",
  },
];

const STATE_PARAM = "state";

export const Instances = () => {
  const { dagId = "", taskId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-start_date";
  const filteredState = searchParams.getAll(STATE_PARAM);
  const hasFilteredState = filteredState.length > 0;

  const { data: task, error: taskError, isLoading: isTaskLoading } = useTaskServiceGetTask({ dagId, taskId });

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.delete(STATE_PARAM);
        value.filter((state) => state !== "all").map((state) => searchParams.append(STATE_PARAM, state));
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const { data, error, isFetching, isLoading } = useTaskInstanceServiceGetTaskInstances({
    dagId,
    dagRunId: "~",
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    state: hasFilteredState ? filteredState : undefined,
    taskId,
  });

  return (
    <Box pt={4}>
      <Flex>
        <Select.Root
          collection={stateOptions}
          maxW="250px"
          multiple
          onValueChange={handleStateChange}
          value={hasFilteredState ? filteredState : ["all"]}
        >
          <Select.Trigger
            {...(hasFilteredState ? { clearable: true } : {})}
            colorPalette="blue"
            isActive={Boolean(filteredState)}
          >
            <Select.ValueText>
              {() =>
                hasFilteredState ? (
                  <HStack gap="10px">
                    {filteredState.map((state) => (
                      <StateBadge key={state} state={state as TaskInstanceState}>
                        {state === "none" ? "No Status" : capitalize(state)}
                      </StateBadge>
                    ))}
                  </HStack>
                ) : (
                  "All States"
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
                  <StateBadge state={option.value as TaskInstanceState}>{option.label}</StateBadge>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Flex>

      <DataTable
        columns={columns(Boolean(task?.is_mapped))}
        data={data?.task_instances ?? []}
        errorMessage={<ErrorAlert error={error ?? taskError} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading || isTaskLoading}
        modelName="Task Instance"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
