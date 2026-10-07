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

import { DagRunStateCounts } from "./DagRunStateCounts";
import { DagTags } from "./DagTags";
import { useDagsListCounts } from "./DagsListCountsContext";
import { buildRecentTaskStateCountsColumns } from "./RecentTaskStateCountsColumn";
import { Schedule } from "./Schedule";

export const getRowKey = (dag: DAGWithLatestDagRunsResponse) => dag.dag_id;

type GetColumnsParams = {
  readonly multiTeam: boolean;
  readonly showRecentTasks: boolean;
};

/**
 * Every cell renderer below is a module-level component rather than an inline arrow.
 * `flexRender` renders them as component types, so a new function identity each render would
 * remount the cell instead of updating it — see `DagsListCountsContext`.
 */
type CellProps = { readonly row: { readonly original: DAGWithLatestDagRunsResponse } };

const SelectHeader = () => <SelectionHeaderCheckbox colorPalette="brand" />;

const SelectCell = ({ row }: CellProps) => (
  <SelectionRowCheckbox colorPalette="brand" rowKey={getRowKey(row.original)} />
);

const PauseCell = ({ row: { original } }: CellProps) => (
  <TogglePause
    dagDisplayName={original.dag_display_name}
    dagId={original.dag_id}
    hasUnfinishedRuns={original.has_unfinished_runs}
    isPaused={original.is_paused}
    schedulingState={original.scheduling_state}
  />
);

const DagNameCell = ({ row: { original } }: CellProps) => (
  <RouterLink fontWeight="bold" to={`/dags/${original.dag_id}`} whiteSpace="nowrap">
    {original.dag_display_name}
  </RouterLink>
);

const ScheduleCell = ({ row: { original } }: CellProps) => (
  <Box whiteSpace="nowrap">
    <Schedule
      assetExpression={original.asset_expression}
      dagId={original.dag_id}
      timetableDescription={original.timetable_description}
      timetablePartitioned={original.timetable_partitioned}
      timetableSummary={original.timetable_summary}
    />
  </Box>
);

const NextRunCell = ({ row: { original } }: CellProps) =>
  original.is_paused ? undefined : original.scheduling_state === "draining" ? (
    <DrainingBadge />
  ) : Boolean(original.next_dagrun_run_after) ? (
    <Box whiteSpace="nowrap">
      <DagRunInfo
        logicalDate={original.next_dagrun_logical_date}
        runAfter={original.next_dagrun_run_after as string}
      />
    </Box>
  ) : undefined;

const LatestRunCell = ({ row: { original } }: CellProps) =>
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
  ) : undefined;

const RunStateCountsCell = ({ row: { original } }: CellProps) => {
  const { runStateCounts } = useDagsListCounts();

  return (
    <DagRunStateCounts
      compact
      counts={runStateCounts.countsByDag[original.dag_id]}
      dagId={original.dag_id}
      isLoading={runStateCounts.isLoading}
      stateCountLimit={runStateCounts.stateCountLimit}
    />
  );
};

const TagsCell = ({
  row: {
    original: { tags },
  },
}: CellProps) => (
  <Box whiteSpace="nowrap">
    <DagTags hideIcon tags={tags} />
  </Box>
);

const TeamNameCell = ({ row: { original } }: CellProps) => (
  <Box whiteSpace="nowrap">
    <TeamName teamName={original.team_name} />
  </Box>
);

const PendingActionsCell = ({ row: { original: dag } }: CellProps) => (
  <NeedsReviewBadge pendingActions={dag.pending_actions} />
);

const TriggerCell = ({ row: { original } }: CellProps) => (
  <TriggerDAGButton
    allowedRunTypes={original.allowed_run_types}
    dagDisplayName={original.dag_display_name}
    dagId={original.dag_id}
  />
);

const FavoriteCell = ({ row: { original } }: CellProps) => (
  <FavoriteDagButton dagId={original.dag_id} isFavorite={original.is_favorite} />
);

const DeleteCell = ({ row: { original } }: CellProps) => (
  <DeleteDagButton dagDisplayName={original.dag_display_name} dagId={original.dag_id} />
);

export const createColumns = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  runStateContext: RunStateCountsContext,
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
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "trigger",
    cell: TriggerCell,
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
    enableSorting: false,
    header: "",
  },
];
