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
import type { ColumnDef } from "@tanstack/react-table";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import type { DataTableFeatures } from "src/components/DataTable/features";

import {
  DagNameCell,
  DeleteCell,
  FavoriteCell,
  LatestRunCell,
  NextRunCell,
  PauseCell,
  PendingActionsCell,
  RunStateCountsCell,
  ScheduleCell,
  SelectCell,
  SelectHeader,
  TagsCell,
  TeamNameCell,
  TriggerCell,
} from "./DagsListCells";
import { buildRecentTaskStateCountsColumns } from "./RecentTaskStateCountsColumn";

type GetColumnsParams = {
  readonly multiTeam: boolean;
  readonly showRecentTasks: boolean;
};

export const createColumns = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  { multiTeam, showRecentTasks }: GetColumnsParams,
): Array<ColumnDef<DataTableFeatures, DAGWithLatestDagRunsResponse>> => [
  {
    accessorKey: "select",
    cell: SelectCell,
    enableHiding: false,
    enableSorting: false,
    header: SelectHeader,
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "is_paused",
    cell: PauseCell,
    enableHiding: false,
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "dag_display_name",
    cell: DagNameCell,
    header: translate("dagId"),
  },
  {
    accessorKey: "timetable_description",
    cell: ScheduleCell,
    enableSorting: false,
    header: translate("dagDetails.schedule"),
  },
  {
    accessorKey: "next_dagrun",
    cell: NextRunCell,
    header: translate("dagDetails.nextRun"),
  },
  {
    accessorKey: "last_run_run_after",
    cell: LatestRunCell,
    header: translate("dagDetails.latestRun"),
  },
  {
    accessorKey: "run_state_counts",
    cell: RunStateCountsCell,
    enableSorting: false,
    header: translate("dags:runStateCounts.label"),
  },
  ...buildRecentTaskStateCountsColumns(showRecentTasks),
  {
    accessorKey: "tags",
    cell: TagsCell,
    enableSorting: false,
    header: translate("dagDetails.tags"),
  },
  ...(multiTeam
    ? [
        {
          accessorKey: "team_name",
          cell: TeamNameCell,
          enableSorting: false,
          header: translate("dagDetails.team"),
        },
      ]
    : []),
  {
    accessorKey: "pending_actions",
    cell: PendingActionsCell,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "trigger",
    cell: TriggerCell,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "favourite",
    cell: FavoriteCell,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "delete",
    cell: DeleteCell,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
];
