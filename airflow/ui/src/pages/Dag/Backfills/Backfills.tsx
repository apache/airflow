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
import Time from "src/components/Time";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";
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
    cell: ({ row }) => (
      <Text>
        {
          reprocessBehaviors.find((rb: { value: string }) => rb.value === row.original.reprocess_behavior)
            ?.label
        }
      </Text>
    ),
    enableSorting: false,
    header: () => "Reprocess Behavior",
  },
  {
    accessorKey: "max_active_runs",
    enableSorting: false,
    header: () => "Max Active Runs",
  },
  {
    accessorKey: "date_range",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.from_date} /> - <Time datetime={row.original.to_date} />
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
        {pluralize("Backfill", data ? data.total_entries : 0)}
      </Heading>
      <DataTable
        columns={columns}
        data={data ? data.backfills : []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Backfill"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
