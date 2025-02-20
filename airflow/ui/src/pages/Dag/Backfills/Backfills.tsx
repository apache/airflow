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
import { Box, Heading, HStack, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { MdOutlineContentCopy } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useBackfillServiceListBackfills } from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { pluralize } from "src/utils";

const columns: Array<ColumnDef<BackfillResponse>> = [
  {
    accessorKey: "id",
    cell: ({ row }) => (
      <HStack>
        <Text>{row.original.id}</Text>
        <MdOutlineContentCopy
          onClick={() => {
            void navigator.clipboard.writeText(row.original.id.toString());
          }}
        />
      </HStack>
    ),
    enableSorting: true,
    header: () => "ID",
  },
  {
    accessorKey: "reprocess_behavior",
    enableSorting: false,
    header: () => "Run Type",
  },
  {
    accessorKey: "max_active_runs",
    enableSorting: false,
    header: () => "Run",
  },
  {
    accessorKey: "date_range",
    cell: ({ row }) => (
      <Text>
        {new Date(row.original.from_date).toUTCString()} - {new Date(row.original.to_date).toUTCString()}
      </Text>
    ),
    enableSorting: false,
    header: () => "Backfill time range",
  },
  {
    accessorKey: "duration",
    cell: ({ row }) => (
      <Text>
        {row.original.completed_at === null
          ? ""
          : new Date(
              Math.abs(
                new Date(row.original.completed_at).getTime() - new Date(row.original.created_at).getTime(),
              ),
            )
              .toISOString()
              .slice(11, 19)}
      </Text>
    ),
    enableSorting: false,
    header: () => "Duration",
  },
];

export const Backfills = () => {
  const { setTableURLState, tableURLState } = useTableURLState();
  // const [searchParams, setSearchParams] = useSearchParams();

  const { pagination, sorting } = tableURLState;

  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-id";

  const { dagId = "" } = useParams();

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfills({
    dagId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  return (
    <Box>
      <ErrorAlert error={error} />
      <Heading my={1} size="md">
        {pluralize("Task", data ? data.total_entries : 0)}
      </Heading>
      <DataTable
        columns={columns}
        data={data ? data.backfills : []}
        displayMode="card"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Task"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
