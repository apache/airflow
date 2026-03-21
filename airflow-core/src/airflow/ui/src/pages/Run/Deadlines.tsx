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
import { Badge } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useDeadlinesServiceGetDagRunDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

const columns = (translate: TFunction): Array<ColumnDef<DeadlineResponse>> => [
  {
    accessorKey: "alert_name",
    header: translate("deadlines.name"),
    id: "alert_name",
  },
  {
    accessorKey: "deadline_time",
    cell: ({ row }) => <Time datetime={row.original.deadline_time} />,
    header: translate("deadlines.deadlineTime"),
    id: "deadline_time",
  },
  {
    accessorKey: "missed",
    cell: ({ row }) => (
      <Badge colorPalette={row.original.missed ? "red" : "green"}>
        {row.original.missed ? translate("deadlines.missed") : translate("deadlines.onTrack")}
      </Badge>
    ),
    enableSorting: false,
    header: translate("deadlines.status"),
    id: "missed",
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => <Time datetime={row.original.created_at} />,
    header: translate("deadlines.createdAt"),
    id: "created_at",
  },
  {
    accessorKey: "alert_description",
    enableSorting: false,
    header: translate("deadlines.description"),
    id: "alert_description",
  },
];

export const Deadlines = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", runId = "" } = useParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["deadline_time"];

  const { data, error, isLoading } = useDeadlinesServiceGetDagRunDeadlines(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    { placeholderData: (prev) => prev },
  );

  return (
    <DataTable
      columns={columns(translate)}
      data={data?.deadlines ?? []}
      errorMessage={<ErrorAlert error={error} />}
      initialState={tableURLState}
      isLoading={isLoading}
      modelName="common:deadline"
      onStateChange={setTableURLState}
      total={data?.total_entries}
    />
  );
};
