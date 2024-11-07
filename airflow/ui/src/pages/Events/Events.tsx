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
import { Box } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useParams } from "react-router-dom";

import { useEventLogServiceGetEventLogs } from "openapi/queries";
import type { EventLogResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

export const Events = () => {
  const { dagId } = useParams();

  const { setTableURLState, tableURLState } = useTableURLState({
    sorting: [{ desc: true, id: "when" }],
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;

  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const {
    data,
    error: EventsError,
    isFetching,
    isLoading,
  } = useEventLogServiceGetEventLogs({
    dagId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const columns: Array<ColumnDef<EventLogResponse>> = [
    {
      accessorKey: "when",
      cell: ({ row: { original } }) => <Time datetime={original.when} />,
      enableSorting: true,
      header: "When",
      meta: {
        skeletonWidth: 10,
      },
    },
    ...(Boolean(dagId)
      ? []
      : [
          {
            accessorKey: "dag_id",
            enableSorting: true,
            header: "Dag ID",
            meta: {
              skeletonWidth: 10,
            },
          },
        ]),
    {
      accessorKey: "run_id",
      enableSorting: true,
      header: "Run ID",
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "task_id",
      enableSorting: true,
      header: "Task ID",
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "map_index",
      enableSorting: false,
      header: "Map Index",
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "try_number",
      enableSorting: false,
      header: "Try Number",
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "event",
      enableSorting: true,
      header: "Event",
      meta: {
        skeletonWidth: 10,
      },
    },
    {
      accessorKey: "owner",
      enableSorting: true,
      header: "User",
      meta: {
        skeletonWidth: 10,
      },
    },
  ];

  return (
    <Box>
      <ErrorAlert error={EventsError} />
      <DataTable
        columns={columns}
        data={data ? data.event_logs : []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Events"
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
