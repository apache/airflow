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

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { DeleteDagButton } from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import DagRunInfo from "src/components/DagRunInfo";
import { SelectionHeaderCheckbox, SelectionRowCheckbox } from "src/components/DataTable/RowSelection";
import { DrainingBadge } from "src/components/DrainingBadge";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
import { PluginActions } from "src/components/PluginActions";
import { TeamName } from "src/components/TeamName";
import { TogglePause } from "src/components/TogglePause";
import { TriggerDAGButton } from "src/components/TriggerDag/TriggerDAGButton";

import { DagRunStateCounts } from "./DagRunStateCounts";
import { DagTags } from "./DagTags";
import { useDagsListCounts } from "./DagsListCountsContext";
import { RecentTaskStateCounts } from "./RecentTaskStateCounts";
import { Schedule } from "./Schedule";
import { getRowKey } from "./dagsListRowKey";

/**
 * Every cell renderer here is a module-level component rather than an inline arrow.
 * `flexRender` renders them as component types, so a new function identity each render would
 * remount the cell instead of updating it — see `DagsListCountsContext`.
 */
type CellProps = { readonly row: { readonly original: DAGWithLatestDagRunsResponse } };

export const SelectHeader = () => <SelectionHeaderCheckbox colorPalette="brand" />;

export const SelectCell = ({ row }: CellProps) => (
  <SelectionRowCheckbox colorPalette="brand" rowKey={getRowKey(row.original)} />
);

export const PauseCell = ({ row: { original } }: CellProps) => (
  <TogglePause
    dagDisplayName={original.dag_display_name}
    dagId={original.dag_id}
    hasUnfinishedRuns={original.has_unfinished_runs}
    isPaused={original.is_paused}
    schedulingState={original.scheduling_state}
  />
);

export const DagNameCell = ({ row: { original } }: CellProps) => (
  <RouterLink fontWeight="bold" to={`/dags/${original.dag_id}`} whiteSpace="nowrap">
    {original.dag_display_name}
  </RouterLink>
);

export const ScheduleCell = ({ row: { original } }: CellProps) => (
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

export const NextRunCell = ({ row: { original } }: CellProps) =>
  original.is_paused ? undefined : original.scheduling_state === "draining" ? (
    <DrainingBadge dagId={original.dag_id} />
  ) : Boolean(original.next_dagrun_run_after) ? (
    <Box whiteSpace="nowrap">
      <DagRunInfo
        logicalDate={original.next_dagrun_logical_date}
        runAfter={original.next_dagrun_run_after as string}
      />
    </Box>
  ) : undefined;

export const LatestRunCell = ({ row: { original } }: CellProps) =>
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

export const RunStateCountsCell = ({ row: { original } }: CellProps) => {
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

export const TagsCell = ({
  row: {
    original: { tags },
  },
}: CellProps) => (
  <Box whiteSpace="nowrap">
    <DagTags hideIcon tags={tags} />
  </Box>
);

export const TeamNameCell = ({ row: { original } }: CellProps) => (
  <Box whiteSpace="nowrap">
    <TeamName teamName={original.team_name} />
  </Box>
);

export const PendingActionsCell = ({ row: { original: dag } }: CellProps) => (
  <NeedsReviewBadge pendingActions={dag.pending_actions} />
);

export const TriggerCell = ({ row: { original } }: CellProps) => (
  <TriggerDAGButton
    allowedRunTypes={original.allowed_run_types}
    dagDisplayName={original.dag_display_name}
    dagId={original.dag_id}
  />
);

export const FavoriteCell = ({ row: { original } }: CellProps) => (
  <FavoriteDagButton dagId={original.dag_id} isFavorite={original.is_favorite} />
);

// Plugin actions sit in this cell rather than a column of their own, so a deployment with no
// Dag actions does not carry a permanently empty column -- and `PluginActions` renders nothing
// when there are none, matching how the card lays them out.
export const DeleteCell = ({ row: { original } }: CellProps) => (
  <>
    <DeleteDagButton dagDisplayName={original.dag_display_name} dagId={original.dag_id} />
    <PluginActions entity={{ dag: original, destination: "dag" }} />
  </>
);

export const RecentTaskStateCountsCell = ({
  row: { original },
}: {
  readonly row: { readonly original: DAGWithLatestDagRunsResponse };
}) => {
  const { recentTasks } = useDagsListCounts();

  return (
    <RecentTaskStateCounts
      compact
      dagId={original.dag_id}
      entry={recentTasks.entriesByDag[original.dag_id]}
      isLoading={recentTasks.isLoading}
    />
  );
};
