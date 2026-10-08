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
import { Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import dayjs from "dayjs";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { FiFileText } from "react-icons/fi";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";

import { type DurationFormat, useAutoRefresh, useDurationFormat } from "src/utils";

const ACTIVE_STATES = new Set(["pending", "queued", "running"]);

// Shared with the callback logs page, which shows the callback's row without the logs column.
export const getCallbackColumns = (
  translate: TFunction,
  runEndDate: string | null | undefined,
  renderDuration: DurationFormat["renderDuration"],
): Array<ColumnDef<DataTableFeatures, DeadlineResponse>> => [
  {
    accessorKey: "callback_path",
    cell: ({ row: { original } }) => <TruncatedText text={original.callback_path ?? original.callback_id} />,
    enableSorting: false,
    header: translate("callbacks.columns.callback"),
  },
  {
    accessorKey: "callback_type",
    cell: ({ row: { original } }) =>
      translate(`callbacks.types.${original.callback_type}`, { defaultValue: original.callback_type }),
    header: translate("callbacks.columns.type"),
  },
  {
    accessorKey: "callback_state",
    cell: ({ row: { original } }) => (
      // "pending" is callback-only, so it borrows the "scheduled" badge style.
      <StateBadge state={original.callback_state === "pending" ? "scheduled" : original.callback_state}>
        {translate(`common:states.${original.callback_state ?? "no_status"}`)}
      </StateBadge>
    ),
    header: translate("common:state"),
  },
  {
    accessorKey: "alert_name",
    cell: ({ row: { original } }) => original.alert_name ?? "",
    enableSorting: false,
    header: translate("callbacks.columns.alertName"),
  },
  {
    accessorKey: "deadline_time",
    cell: ({ row: { original } }) => <Time datetime={original.deadline_time} />,
    header: translate("callbacks.columns.deadlineTime"),
  },
  {
    accessorKey: "missed_by",
    // How late the run finished relative to the deadline, like the run header's deadline badge.
    cell: ({ row: { original } }) => {
      if (!original.missed) {
        return "-";
      }
      if (runEndDate === null || runEndDate === undefined) {
        return translate("deadlineStatus.stillRunning");
      }
      const diff = dayjs(runEndDate).diff(dayjs(original.deadline_time));

      return diff < 0 ? "-" : (renderDuration(diff / 1000) ?? "-");
    },
    enableSorting: false,
    header: translate("callbacks.columns.missedBy"),
  },
  {
    accessorKey: "logs",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info">
        <RouterLink to={`${original.callback_id}/logs`}>
          <FiFileText />
          {translate("tabs.logs")}
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("tabs.logs"),
  },
];

export const Callbacks = () => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const { dagId = "", runId = "" } = useParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const refetchInterval = useAutoRefresh({ dagId });
  const { renderDuration } = useDurationFormat();
  // Already loaded (and refreshed while running) by the Run page.
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId });

  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;

  const { data, error, isFetching, isLoading } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.deadlines.some(({ callback_state: state }) => ACTIVE_STATES.has(state ?? "")) &&
        refetchInterval,
    },
  );

  return (
    <DataTable
      columns={getCallbackColumns(translate, dagRun?.end_date, renderDuration)}
      data={data?.deadlines ?? []}
      errorMessage={<ErrorAlert error={error} />}
      initialState={tableURLState}
      isFetching={isFetching}
      isLoading={isLoading}
      modelName="dag:callbacks.callback"
      onStateChange={setTableURLState}
      total={data?.total_entries}
    />
  );
};
