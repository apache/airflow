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

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { DeleteDagButton } from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import DagRunInfo from "src/components/DagRunInfo";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { SelectionHeaderCheckbox, SelectionRowCheckbox } from "src/components/DataTable/useRowSelection";
import { DrainingBadge } from "src/components/DrainingBadge";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
import { TeamName } from "src/components/TeamName";
import { TogglePause } from "src/components/TogglePause";
import { TriggerDAGButton } from "src/components/TriggerDag/TriggerDAGButton";

import type { RecentTasks } from "src/queries/useRecentTaskStateCounts";

import { DagRunStateCounts } from "./DagRunStateCounts";
import { DagTags } from "./DagTags";
import { buildRecentTaskStateCountsColumns } from "./RecentTaskStateCountsColumn";
import { Schedule } from "./Schedule";

export const getRowKey = (dag: DAGWithLatestDagRunsResponse) => dag.dag_id;

type GetColumnsParams = {
  readonly multiTeam: boolean;
  readonly recentTasks: RecentTasks;
};

export type RunStateCountsContext = {
  readonly countsByDag: Record<string, Record<string, number> | undefined>;
  readonly isLoading: boolean;
  readonly stateCountLimit: number | undefined;
};

export const createColumns = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  runStateContext: RunStateCountsContext,
  { multiTeam, recentTasks }: GetColumnsParams,
): Array<ColumnDef<DataTableFeatures, DAGWithLatestDagRunsResponse>> => [
  {
    accessorKey: "select",
    cell: ({ row }) => <SelectionRowCheckbox colorPalette="brand" rowKey={getRowKey(row.original)} />,
    enableHiding: false,
    enableSorting: false,
    header: () => <SelectionHeaderCheckbox colorPalette="brand" />,
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "is_paused",
    cell: ({ row: { original } }) => (
      <TogglePause
        dagDisplayName={original.dag_display_name}
        dagId={original.dag_id}
        hasUnfinishedRuns={original.has_unfinished_runs}
        isPaused={original.is_paused}
        schedulingState={original.scheduling_state}
      />
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "dag_display_name",
    cell: ({ row: { original } }) => (
      <RouterLink fontWeight="bold" to={`/dags/${original.dag_id}`} whiteSpace="nowrap">
        {original.dag_display_name}
      </RouterLink>
    ),
    header: () => translate("dagId"),
  },
  {
    accessorKey: "timetable_description",
    cell: ({ row: { original } }) => (
      <Box whiteSpace="nowrap">
        <Schedule
          assetExpression={original.asset_expression}
          dagId={original.dag_id}
          timetableDescription={original.timetable_description}
          timetablePartitioned={original.timetable_partitioned}
          timetableSummary={original.timetable_summary}
        />
      </Box>
    ),
    enableSorting: false,
    header: () => translate("dagDetails.schedule"),
  },
  {
    accessorKey: "next_dagrun",
    cell: ({ row: { original } }) =>
      original.is_paused ? undefined : original.scheduling_state === "draining" ? (
        <DrainingBadge dagId={original.dag_id} />
      ) : Boolean(original.next_dagrun_run_after) ? (
        <Box whiteSpace="nowrap">
          <DagRunInfo
            logicalDate={original.next_dagrun_logical_date}
            runAfter={original.next_dagrun_run_after as string}
          />
        </Box>
      ) : undefined,
    header: () => translate("dagDetails.nextRun"),
  },
  {
    accessorKey: "last_run_run_after",
    cell: ({ row: { original } }) =>
      original.latest_dag_runs[0] ? (
        <RouterLink
          fontWeight="bold"
          to={`/dags/${original.dag_id}/runs/${original.latest_dag_runs[0].run_id}`}
          whiteSpace="nowrap"
        >
          <DagRunInfo
            endDate={original.latest_dag_runs[0].end_date}
            logicalDate={original.latest_dag_runs[0].logical_date}
            runAfter={original.latest_dag_runs[0].run_after}
            startDate={original.latest_dag_runs[0].start_date}
            state={original.latest_dag_runs[0].state}
          />
        </RouterLink>
      ) : undefined,
    header: () => translate("dagDetails.latestRun"),
  },
  {
    accessorKey: "run_state_counts",
    cell: ({ row: { original } }) => (
      <DagRunStateCounts
        compact
        counts={runStateContext.countsByDag[original.dag_id]}
        dagId={original.dag_id}
        isLoading={runStateContext.isLoading}
        stateCountLimit={runStateContext.stateCountLimit}
      />
    ),
    enableSorting: false,
    header: () => translate("dags:runStateCounts.label"),
  },
  ...buildRecentTaskStateCountsColumns(recentTasks),
  {
    accessorKey: "tags",
    cell: ({
      row: {
        original: { tags },
      },
    }) => (
      <Box whiteSpace="nowrap">
        <DagTags hideIcon tags={tags} />
      </Box>
    ),
    enableSorting: false,
    header: () => translate("dagDetails.tags"),
  },
  ...(multiTeam
    ? [
        {
          accessorKey: "team_name",
          cell: ({ row: { original } }: { row: { original: DAGWithLatestDagRunsResponse } }) => (
            <Box whiteSpace="nowrap">
              <TeamName teamName={original.team_name} />
            </Box>
          ),
          enableSorting: false,
          header: () => translate("dagDetails.team"),
        },
      ]
    : []),
  {
    accessorKey: "pending_actions",
    cell: ({ row: { original: dag } }) => <NeedsReviewBadge pendingActions={dag.pending_actions} />,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "trigger",
    cell: ({ row: { original } }) => (
      <TriggerDAGButton
        allowedRunTypes={original.allowed_run_types}
        dagDisplayName={original.dag_display_name}
        dagId={original.dag_id}
      />
    ),
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "favourite",
    cell: ({ row: { original } }) => (
      <FavoriteDagButton dagId={original.dag_id} isFavorite={original.is_favorite} />
    ),
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "delete",
    cell: ({ row: { original } }) => (
      <DeleteDagButton dagDisplayName={original.dag_display_name} dagId={original.dag_id} />
    ),
    enableSorting: false,
    header: "",
  },
];
