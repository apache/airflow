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
  Flex,
  Link,
  createListCollection,
  HStack,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback, useState } from "react";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import { SearchBar } from "src/components/SearchBar";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { capitalize, getDuration } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

const columns: Array<ColumnDef<TaskInstanceResponse>> = [
  {
    accessorKey: "task_display_name",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={getTaskInstanceLink(original)}>{original.task_display_name}</RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: "Task ID",
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
  {
    accessorFn: (row: TaskInstanceResponse) => row.rendered_map_index ?? row.map_index,
    header: "Map Index",
  },

  {
    accessorKey: "try_number",
    enableSorting: false,
    header: "Try Number",
  },
  {
    accessorKey: "operator",
    enableSorting: false,
    header: "Operator",
  },

  {
    cell: ({ row: { original } }) => `${getDuration(original.start_date, original.end_date)}s`,
    header: "Duration",
  },
  {
    accessorKey: "actions",
    cell: ({ row }) => (
      <Flex justifyContent="end">
        <ClearTaskInstanceButton taskInstance={row.original} withText={false} />
        <MarkTaskInstanceAsButton taskInstance={row.original} withText={false} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

const stateOptions = createListCollection<{ label: string; value: TaskInstanceState | "all" | "none" }>({
  items: [
    { label: "All States", value: "all" },
    { label: "Scheduled", value: "scheduled" },
    { label: "Queued", value: "queued" },
    { label: "Running", value: "running" },
    { label: "Success", value: "success" },
    { label: "Restarting", value: "restarting" },
    { label: "Failed", value: "failed" },
    { label: "Up For Retry", value: "up_for_retry" },
    { label: "Up For Reschedule", value: "up_for_reschedule" },
    { label: "Upstream failed", value: "upstream_failed" },
    { label: "Skipped", value: "skipped" },
    { label: "Deferred", value: "deferred" },
    { label: "Removed", value: "removed" },
    { label: "No Status", value: "none" },
  ],
});

const STATE_PARAM = "state";

export const TaskInstances = () => {
  const { dagId = "", runId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-start_date";
  const filteredState = searchParams.getAll(STATE_PARAM);
  const hasFilteredState = filteredState.length > 0;
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;

  const [taskDisplayNamePattern, setTaskDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

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

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setTaskDisplayNamePattern(value);
    setSearchParams(searchParams);
  };

  const { data, error, isFetching, isLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      state: hasFilteredState ? filteredState : undefined,
      taskDisplayNamePattern: Boolean(taskDisplayNamePattern) ? taskDisplayNamePattern : undefined,
    },
    undefined,
    { enabled: !isNaN(pagination.pageSize) },
  );

  return (
    <Box pt={4}>
      <HStack>
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
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={taskDisplayNamePattern ?? ""}
          hideAdvanced
          onChange={handleSearchChange}
          placeHolder="Search Tasks"
        />
      </HStack>
      <DataTable
        columns={columns}
        data={data?.task_instances ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Task Instance"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
