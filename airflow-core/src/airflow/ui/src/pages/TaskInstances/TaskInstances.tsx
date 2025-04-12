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
import { Flex, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useState } from "react";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getDuration, useAutoRefresh, isStatePending } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

import { TaskInstancesFilter } from "./TaskInstancesFilter";

type TaskInstanceRow = { row: { original: TaskInstanceResponse } };

const {
  END_DATE: END_DATE_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const taskInstanceColumns = (
  dagId?: string,
  runId?: string,
  taskId?: string,
): Array<ColumnDef<TaskInstanceResponse>> => [
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_id",
          enableSorting: false,
          header: "Dag ID",
        },
      ]),
  ...(Boolean(runId)
    ? []
    : [
        {
          accessorKey: "run_after",
          // If we don't show the taskId column, make the dag run a link to the task instance
          cell: ({ row: { original } }: TaskInstanceRow) =>
            Boolean(taskId) ? (
              <Link asChild color="fg.info" fontWeight="bold">
                <RouterLink to={getTaskInstanceLink(original)}>
                  <Time datetime={original.run_after} />
                </RouterLink>
              </Link>
            ) : (
              <Time datetime={original.run_after} />
            ),
          header: "Dag Run",
        },
      ]),
  ...(Boolean(taskId)
    ? []
    : [
        {
          accessorKey: "task_display_name",
          cell: ({ row: { original } }: TaskInstanceRow) => (
            <Link asChild color="fg.info" fontWeight="bold">
              <RouterLink to={getTaskInstanceLink(original)}>
                <TruncatedText text={original.task_display_name} />
              </RouterLink>
            </Link>
          ),
          enableSorting: false,
          header: "Task ID",
        },
      ]),
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
    cell: ({ row: { original } }) =>
      Boolean(taskId) && Boolean(runId) ? (
        <Link asChild color="fg.info" fontWeight="bold">
          <RouterLink to={getTaskInstanceLink(original)}>
            <Time datetime={original.start_date} />
          </RouterLink>
        </Link>
      ) : (
        <Time datetime={original.start_date} />
      ),
    header: "Start Date",
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: "End Date",
  },
  {
    accessorKey: "rendered_map_index",
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
    cell: ({ row: { original } }) =>
      Boolean(original.start_date) ? `${getDuration(original.start_date, original.end_date)}s` : "",
    header: "Duration",
  },
  {
    accessorKey: "dag_version",
    cell: ({ row: { original } }) => <DagVersion version={original.dag_version} />,
    enableSorting: false,
    header: "Dag Version",
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

export const TaskInstances = () => {
  const { dagId, runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-start_date";

  const filteredState = searchParams.getAll(STATE_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const hasFilteredState = filteredState.length > 0;

  const [taskDisplayNamePattern, setTaskDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId: dagId ?? "~",
      dagRunId: runId ?? "~",
      endDateLte: endDate ?? undefined,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      startDateGte: startDate ?? undefined,
      state: hasFilteredState ? filteredState : undefined,
      taskDisplayNamePattern: Boolean(taskDisplayNamePattern) ? taskDisplayNamePattern : undefined,
      taskId: taskId ?? undefined,
    },
    undefined,
    {
      enabled: !isNaN(pagination.pageSize),
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  return (
    <>
      <TaskInstancesFilter
        setTaskDisplayNamePattern={setTaskDisplayNamePattern}
        taskDisplayNamePattern={taskDisplayNamePattern}
      />
      <DataTable
        columns={taskInstanceColumns(dagId, runId, taskId)}
        data={data?.task_instances ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="Task Instance"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </>
  );
};
