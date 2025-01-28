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
import { Box, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances, useTaskServiceGetTask } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { getDuration } from "src/utils";
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

export const Instances = () => {
  const { dagId = "", taskId } = useParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-start_date";

  const { data: task, error: taskError, isLoading: isTaskLoading } = useTaskServiceGetTask({ dagId, taskId });

  const { data, error, isFetching, isLoading } = useTaskInstanceServiceGetTaskInstances({
    dagId,
    dagRunId: "~",
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    taskId,
  });

  return (
    <Box>
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
