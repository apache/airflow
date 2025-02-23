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
import { useCallback, useState } from "react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { DagRunState, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import DagRunInfo from "src/components/DagRunInfo";
import { DataTable } from "src/components/DataTable";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import type { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { TogglePause } from "src/components/TogglePause";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { DagsLayout } from "src/layouts/DagsLayout";
import { useConfig } from "src/queries/useConfig";
import { useDags } from "src/queries/useDags";
import { pluralize } from "src/utils";

import { DAGImportErrors } from "../Dashboard/Stats/DAGImportErrors";
import { DagCard } from "./DagCard";
import { DagTags } from "./DagTags";
import { DagsFilters } from "./DagsFilters";
import { Schedule } from "./Schedule";
import { SortSelect } from "./SortSelect";

const columns: Array<ColumnDef<DAGWithLatestDagRunsResponse>> = [
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
    accessorKey: "dag_id",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}`}>{original.dag_display_name}</RouterLink>
      </Link>
    ),
    header: "Dag",
  },
  {
    accessorKey: "timetable_description",
    cell: ({ row: { original } }) => <Schedule dag={original} />,
    enableSorting: false,
    header: () => "Schedule",
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
    header: "Next Dag Run",
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
    header: "Last Dag Run",
  },
  {
    accessorKey: "tags",
    cell: ({
      row: {
        original: { tags },
      },
    }) => <DagTags hideIcon tags={tags} />,
    enableSorting: false,
    header: () => "Tags",
  },
  {
    accessorKey: "trigger",
    cell: ({ row }) => <TriggerDAGButton dag={row.original} withText={false} />,
    enableSorting: false,
    header: "",
  },
];

const {
  LAST_DAG_RUN_STATE: LAST_DAG_RUN_STATE_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  PAUSED: PAUSED_PARAM,
  TAGS: TAGS_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const cardDef: CardDef<DAGWithLatestDagRunsResponse> = {
  card: ({ row }) => <DagCard dag={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
};

const DAGS_LIST_DISPLAY = "dags_list_display";

export const DagsList = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [display, setDisplay] = useLocalStorage<"card" | "table">(DAGS_LIST_DISPLAY, "card");

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? false : undefined;

  const showPaused = searchParams.get(PAUSED_PARAM);

  const lastDagRunState = searchParams.get(LAST_DAG_RUN_STATE_PARAM) as DagRunState;
  const selectedTags = searchParams.getAll(TAGS_PARAM);

  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination, sorting } = tableURLState;
  const [dagDisplayNamePattern, setDagDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-last_run_start_date";

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setSearchParams(searchParams);
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setDagDisplayNamePattern(value);
  };

  let paused = defaultShowPaused;

  if (showPaused === "all") {
    paused = undefined;
  } else if (showPaused === "true") {
    paused = true;
  } else if (showPaused === "false") {
    paused = false;
  }

  const { data, error, isLoading } = useDags({
    dagDisplayNamePattern: Boolean(dagDisplayNamePattern) ? `${dagDisplayNamePattern}` : undefined,
    lastDagRunState,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    onlyActive: true,
    orderBy,
    paused,
    tags: selectedTags,
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
          defaultValue={dagDisplayNamePattern ?? ""}
          onChange={handleSearchChange}
          placeHolder="Search Dags"
        />
        <DagsFilters />
        <HStack justifyContent="space-between">
          <HStack>
            <Heading py={3} size="md">
              {pluralize("Dag", data.total_entries)}
            </Heading>
            <DAGImportErrors iconOnly />
          </HStack>
          {display === "card" ? (
            <SortSelect handleSortChange={handleSortChange} orderBy={orderBy} />
          ) : undefined}
        </HStack>
      </VStack>
      <ToggleTableDisplay display={display} setDisplay={setDisplay} />
      <Box overflow="auto">
        <DataTable
          cardDef={cardDef}
          columns={columns}
          data={data.dags}
          displayMode={display}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName="Dag"
          onStateChange={setTableURLState}
          skeletonCount={display === "card" ? 5 : undefined}
          total={data.total_entries}
        />
      </Box>
    </DagsLayout>
  );
};
