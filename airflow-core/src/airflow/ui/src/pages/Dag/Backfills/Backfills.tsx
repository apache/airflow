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
import { Box, Heading, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useBackfillServiceListBackfillsUi } from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { reprocessBehaviors } from "src/constants/reprocessBehaviourParams";
import { getDuration } from "src/utils";

const getColumns = (translate: (key: string) => string): Array<ColumnDef<BackfillResponse>> => [
  {
    accessorKey: "date_from",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.from_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.from"),
  },
  {
    accessorKey: "date_to",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.to_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.to"),
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
    header: translate("table.reprocessBehavior"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.created_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.createdAt"),
  },
  {
    accessorKey: "completed_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.completed_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.completedAt"),
  },
  {
    accessorKey: "duration",
    cell: ({ row }) => (
      <Text>
        {row.original.completed_at === null
          ? ""
          : getDuration(row.original.created_at, row.original.completed_at)}
      </Text>
    ),
    enableSorting: false,
    header: translate("table.duration"),
  },
  {
    accessorKey: "max_active_runs",
    enableSorting: false,
    header: translate("table.maxActiveRuns"),
  },
];

export const Backfills = () => {
  const { t: translate } = useTranslation();
  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination } = tableURLState;

  const { dagId = "" } = useParams();

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillsUi({
    dagId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  return (
    <Box>
      <ErrorAlert error={error} />
      <Heading my={1} size="md">
        {translate("backfill", { count: data ? data.total_entries : 0 })}
      </Heading>
      <DataTable
        columns={getColumns(translate)}
        data={data ? data.backfills : []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("backfill_one")}
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
