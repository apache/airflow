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
import { Heading, HStack, Skeleton, VStack, type SelectValueChangeDetails, Box } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { DagRunState, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { DeleteDagButton } from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import DagRunInfo from "src/components/DagRunInfo";
import { DataTable } from "src/components/DataTable";
import type { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
import { SearchBar } from "src/components/SearchBar";
import { TogglePause } from "src/components/TogglePause";
import { TriggerDAGButton } from "src/components/TriggerDag/TriggerDAGButton";
import { RouterLink } from "src/components/ui";
import { DAGS_LIST_DISPLAY_KEY } from "src/constants/localStorage";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAdvancedSearch } from "src/hooks/useAdvancedSearch";
import { DagsLayout } from "src/layouts/DagsLayout";
import { useConfig } from "src/queries/useConfig";
import { useDagRunStateCounts } from "src/queries/useDagRunStateCounts";
import { useDags } from "src/queries/useDags";
import { useDocumentTitle } from "src/utils";

import { DagImportErrors } from "../Dashboard/Stats/DagImportErrors";
import { DagCard } from "./DagCard";
import { DagRunStateCounts } from "./DagRunStateCounts";
import { DagTags } from "./DagTags";
import { DagsFilters } from "./DagsFilters";
import { Schedule } from "./Schedule";
import { SortSelect } from "./SortSelect";
import { useTagFilter } from "./useTagFilter";

type RunStateCountsContext = {
  readonly countsByDag: Record<string, Record<string, number> | undefined>;
  readonly isLoading: boolean;
  readonly stateCountLimit: number | undefined;
};

const createColumns = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  runStateContext: RunStateCountsContext,
): Array<ColumnDef<DAGWithLatestDagRunsResponse>> => [
  {
    accessorKey: "is_paused",
    cell: ({ row: { original } }) => (
      <TogglePause
        dagDisplayName={original.dag_display_name}
        dagId={original.dag_id}
        isPaused={original.is_paused}
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
      <RouterLink fontWeight="bold" to={`/dags/${original.dag_id}`}>
        {original.dag_display_name}
      </RouterLink>
    ),
    header: () => translate("dagId"),
  },
  {
    accessorKey: "timetable_description",
    cell: ({ row: { original } }) => (
      <Schedule
        assetExpression={original.asset_expression}
        dagId={original.dag_id}
        timetableDescription={original.timetable_description}
        timetablePartitioned={original.timetable_partitioned}
        timetableSummary={original.timetable_summary}
      />
    ),
    enableSorting: false,
    header: () => translate("dagDetails.schedule"),
  },
  {
    accessorKey: "next_dagrun",
    cell: ({ row: { original } }) =>
      !original.is_paused && Boolean(original.next_dagrun_run_after) ? (
        <DagRunInfo
          logicalDate={original.next_dagrun_logical_date}
          runAfter={original.next_dagrun_run_after as string}
        />
      ) : undefined,
    header: () => translate("dagDetails.nextRun"),
  },
  {
    accessorKey: "last_run_start_date",
    cell: ({ row: { original } }) =>
      original.latest_dag_runs[0] ? (
        <RouterLink
          fontWeight="bold"
          to={`/dags/${original.dag_id}/runs/${original.latest_dag_runs[0].run_id}`}
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
  {
    accessorKey: "tags",
    cell: ({
      row: {
        original: { tags },
      },
    }) => <DagTags hideIcon tags={tags} />,
    enableSorting: false,
    header: () => translate("dagDetails.tags"),
  },
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
        isPaused={original.is_paused}
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

const {
  DAG_RUN_STATE,
  FAVORITE,
  LAST_DAG_RUN_STATE,
  NAME_PATTERN,
  NEEDS_REVIEW,
  OFFSET,
  OWNERS,
  PAUSED,
}: SearchParamsKeysType = SearchParamsKeys;

const createCardDef = (runStateContext: RunStateCountsContext): CardDef<DAGWithLatestDagRunsResponse> => ({
  card: ({ row }) => (
    <DagCard
      dag={row}
      runStateCounts={runStateContext.countsByDag[row.dag_id]}
      runStateCountsLoading={runStateContext.isLoading}
      stateCountLimit={runStateContext.stateCountLimit}
    />
  ),
  meta: {
    customSkeleton: <Skeleton height="140px" width="100%" />,
  },
});

export const DagsList = () => {
  const { t: translate } = useTranslation();

  useDocumentTitle(translate("common:nav.dags"));

  const [searchParams, setSearchParams] = useSearchParams();
  const [display, setDisplay] = useLocalStorage<"card" | "table">(DAGS_LIST_DISPLAY_KEY, "card");
  const dagRunsLimit = display === "card" ? 14 : 1;

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? false : undefined;

  const showPaused = searchParams.get(PAUSED);
  const showFavorites = searchParams.get(FAVORITE);

  const lastDagRunState = searchParams.get(LAST_DAG_RUN_STATE) as DagRunState;
  const dagRunState = searchParams.get(DAG_RUN_STATE) as DagRunState;
  const { selectedTags, tagFilterMode: selectedMatchMode } = useTagFilter();
  const pendingReviews = searchParams.get(NEEDS_REVIEW);
  const owners = searchParams.getAll(OWNERS);

  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination, sorting } = tableURLState;
  const dagDisplayNamePattern = searchParams.get(NAME_PATTERN) ?? "";
  const advancedSearch = useAdvancedSearch("dags");

  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "dag_display_name";

  const handleSearchChange = (value: string) => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    if (value) {
      searchParams.set(NAME_PATTERN, value);
    } else {
      searchParams.delete(NAME_PATTERN);
    }
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  let paused = defaultShowPaused;
  let isFavorite = undefined;
  let pendingHitl = undefined;

  if (showPaused === "all") {
    paused = undefined;
  } else if (showPaused === "true") {
    paused = true;
  } else if (showPaused === "false") {
    paused = false;
  }

  if (showFavorites === "true") {
    isFavorite = true;
  } else if (showFavorites === "false") {
    isFavorite = false;
  }

  if (pendingReviews === "true") {
    pendingHitl = true;
  } else if (pendingReviews === "false") {
    pendingHitl = false;
  }

  const { data, error, isLoading } = useDags({
    advancedSearch: advancedSearch.enabled,
    dagDisplayNamePattern: Boolean(dagDisplayNamePattern) ? dagDisplayNamePattern : undefined,
    dagRunsLimit,
    dagRunState,
    isFavorite,
    lastDagRunState,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy: [orderBy],
    owners,
    paused,
    pendingHitl,
    tags: selectedTags,
    tagsMatchMode: selectedMatchMode,
  });

  const { data: runStateCountsData, isLoading: runStateCountsLoading } = useDagRunStateCounts({
    dagIds: data?.dags.map((dag) => dag.dag_id) ?? [],
    dags: data?.dags,
  });
  const runStateContext: RunStateCountsContext = {
    countsByDag: Object.fromEntries(
      (runStateCountsData?.dags ?? []).map((entry) => [entry.dag_id, entry.state_counts]),
    ),
    isLoading: runStateCountsLoading,
    stateCountLimit: runStateCountsData?.state_count_limit,
  };

  const columns = createColumns(translate, runStateContext);
  const cardDef = createCardDef(runStateContext);

  const handleSortChange = ({ value }: SelectValueChangeDetails<Array<string>>) => {
    setTableURLState({
      pagination,
      sorting: value.map((val) => ({
        desc: val.startsWith("-"),
        id: val.replace("-", ""),
      })),
    });
  };

  const totalEntries = data?.total_entries ?? 0;

  return (
    <DagsLayout>
      <VStack alignItems="none">
        <SearchBar
          advancedSearch={advancedSearch}
          defaultValue={dagDisplayNamePattern}
          onChange={handleSearchChange}
          placeholder={translate("dags:search.dags")}
        />
        <DagsFilters />
        <HStack justifyContent="space-between">
          <HStack>
            <Heading py={3} size="md">
              {`${totalEntries} ${translate("dag", { count: totalEntries })}`}
            </Heading>
            <DagImportErrors iconOnly />
          </HStack>
          <HStack>
            {display === "card" ? (
              <SortSelect handleSortChange={handleSortChange} orderBy={orderBy} />
            ) : undefined}
          </HStack>
        </HStack>
      </VStack>
      <Box pb={8}>
        <DataTable
          cardDef={cardDef}
          columns={columns}
          data={data?.dags ?? []}
          displayMode={display}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName="common:dag"
          onDisplayToggleChange={setDisplay}
          onStateChange={setTableURLState}
          showDisplayToggle
          showRowCountHeading={false}
          skeletonCount={display === "card" ? 5 : undefined}
          total={totalEntries}
        />
      </Box>
    </DagsLayout>
  );
};
