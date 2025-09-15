/* eslint-disable max-lines */

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
import {
  Heading,
  HStack,
  Skeleton,
  VStack,
  Link,
  type SelectValueChangeDetails,
  Box,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { DagRunState, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import DeleteDagButton from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import DagRunInfo from "src/components/DagRunInfo";
import { DataTable } from "src/components/DataTable";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import type { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
import { SearchBar } from "src/components/SearchBar";
import { TogglePause } from "src/components/TogglePause";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { SearchParamsKeys } from "src/constants/searchParams";
import { DagsLayout } from "src/layouts/DagsLayout";
import { useConfig } from "src/queries/useConfig";
import { useDags } from "src/queries/useDags";

import { DAGImportErrors } from "../Dashboard/Stats/DAGImportErrors";
import { DagCard } from "./DagCard";
import { DagTags } from "./DagTags";
import { DagsFilters } from "./DagsFilters";
import { Schedule } from "./Schedule";
import { SortSelect } from "./SortSelect";

const createColumns = (
  translate: (key: string, options?: Record<string, unknown>) => string,
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
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}`}>{original.dag_display_name}</RouterLink>
      </Link>
    ),
    header: () => translate("dagId"),
  },
  {
    accessorKey: "timetable_description",
    cell: ({ row: { original } }) => {
      const [latestRun] = original.latest_dag_runs;

      return (
        <Schedule
          assetExpression={original.asset_expression}
          dagId={original.dag_id}
          latestRunAfter={latestRun?.run_after}
          timetableDescription={original.timetable_description}
          timetableSummary={original.timetable_summary}
        />
      );
    },
    enableSorting: false,
    header: () => translate("dagDetails.schedule"),
  },
  {
    accessorKey: "next_dagrun",
    cell: ({ row: { original } }) =>
      Boolean(original.next_dagrun_run_after) ? (
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
        <Link asChild color="fg.info" fontWeight="bold">
          <RouterLink to={`/dags/${original.dag_id}/runs/${original.latest_dag_runs[0].dag_run_id}`}>
            <DagRunInfo
              endDate={original.latest_dag_runs[0].end_date}
              logicalDate={original.latest_dag_runs[0].logical_date}
              runAfter={original.latest_dag_runs[0].run_after}
              startDate={original.latest_dag_runs[0].start_date}
              state={original.latest_dag_runs[0].state}
            />
          </RouterLink>
        </Link>
      ) : undefined,
    header: () => translate("dagDetails.latestRun"),
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
    cell: ({ row: { original: dag } }) => (
      <NeedsReviewBadge dagId={dag.dag_id} pendingActions={dag.pending_actions} />
    ),
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "trigger",
    cell: ({ row: { original } }) => (
      <TriggerDAGButton
        dagDisplayName={original.dag_display_name}
        dagId={original.dag_id}
        isPaused={original.is_paused}
        withText={false}
      />
    ),
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "favorite",
    cell: ({ row: { original } }) => <FavoriteDagButton dagId={original.dag_id} withText={false} />,
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
  {
    accessorKey: "delete",
    cell: ({ row: { original } }) => (
      <DeleteDagButton dagDisplayName={original.dag_display_name} dagId={original.dag_id} withText={false} />
    ),
    enableSorting: false,
    header: "",
  },
];

const { FAVORITE, LAST_DAG_RUN_STATE, NAME_PATTERN, NEEDS_REVIEW, OWNERS, PAUSED, TAGS, TAGS_MATCH_MODE } =
  SearchParamsKeys;

const cardDef: CardDef<DAGWithLatestDagRunsResponse> = {
  card: ({ row }) => <DagCard dag={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
};

const DAGS_LIST_DISPLAY = "dags_list_display";

export const DagsList = () => {
  const { t: translate } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [display, setDisplay] = useLocalStorage<"card" | "table">(DAGS_LIST_DISPLAY, "card");
  const dagRunsLimit = display === "card" ? 14 : 1;

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? false : undefined;

  const showPaused = searchParams.get(PAUSED);
  const showFavorites = searchParams.get(FAVORITE);

  const lastDagRunState = searchParams.get(LAST_DAG_RUN_STATE) as DagRunState;
  const selectedTags = searchParams.getAll(TAGS);
  const selectedMatchMode = searchParams.get(TAGS_MATCH_MODE) as "all" | "any";
  const pendingReviews = searchParams.get(NEEDS_REVIEW);
  const owners = searchParams.getAll(OWNERS);

  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination, sorting } = tableURLState;
  const dagDisplayNamePattern = searchParams.get(NAME_PATTERN) ?? "";

  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "dag_display_name";

  const columns = useMemo(() => createColumns(translate), [translate]);

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
    dagDisplayNamePattern: Boolean(dagDisplayNamePattern) ? dagDisplayNamePattern : undefined,
    dagRunsLimit,
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

  const handleSortChange = useCallback(
    ({ value }: SelectValueChangeDetails<Array<string>>) => {
      setTableURLState({
        pagination,
        sorting: value.map((val) => ({
          desc: val.startsWith("-"),
          id: val.replace("-", ""),
        })),
      });
    },
    [pagination, setTableURLState],
  );

  return (
    <DagsLayout>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={dagDisplayNamePattern}
          onChange={handleSearchChange}
          placeHolder={translate("dags:search.dags")}
        />
        <DagsFilters />
        <HStack justifyContent="space-between">
          <HStack>
            <Heading py={3} size="md">
              {`${data?.total_entries ?? 0} ${translate("dag", { count: data?.total_entries ?? 0 })}`}
            </Heading>
            <DAGImportErrors iconOnly />
          </HStack>
          {display === "card" ? (
            <SortSelect handleSortChange={handleSortChange} orderBy={orderBy} />
          ) : undefined}
        </HStack>
      </VStack>
      <ToggleTableDisplay display={display} setDisplay={setDisplay} />
      <Box overflow="auto" pb={8}>
        <DataTable
          cardDef={cardDef}
          columns={columns}
          data={data?.dags ?? []}
          displayMode={display}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName={translate("dag_one")}
          onStateChange={setTableURLState}
          skeletonCount={display === "card" ? 5 : undefined}
          total={data?.total_entries ?? 0}
        />
      </Box>
    </DagsLayout>
  );
};
