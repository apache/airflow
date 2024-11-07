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
  createListCollection,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { type ChangeEvent, useCallback, useState } from "react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type {
  DagRunState,
  DAGWithLatestDagRunsResponse,
} from "openapi/requests/types.gen";
import DagRunInfo from "src/components/DagRunInfo";
import { DataTable } from "src/components/DataTable";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import type { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { TogglePause } from "src/components/TogglePause";
import { Select } from "src/components/ui";
import {
  SearchParamsKeys,
  type SearchParamsKeysType,
} from "src/constants/searchParams";
import { useDags } from "src/queries/useDags";
import { pluralize } from "src/utils";

import { DagCard } from "./DagCard";
import { DagTags } from "./DagTags";
import { DagsFilters } from "./DagsFilters";
import { Schedule } from "./Schedule";

const columns: Array<ColumnDef<DAGWithLatestDagRunsResponse>> = [
  {
    accessorKey: "is_paused",
    cell: ({ row: { original } }) => (
      <TogglePause dagId={original.dag_id} isPaused={original.is_paused} />
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
        <RouterLink to={`/dags/${original.dag_id}`}>
          {original.dag_display_name}
        </RouterLink>
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
      Boolean(original.next_dagrun) ? (
        <DagRunInfo
          dataIntervalEnd={original.next_dagrun_data_interval_end}
          dataIntervalStart={original.next_dagrun_data_interval_start}
          nextDagrunCreateAfter={original.next_dagrun_create_after}
        />
      ) : undefined,
    enableSorting: false,
    header: "Next Dag Run",
  },
  {
    accessorKey: "latest_dag_runs",
    cell: ({ row: { original } }) =>
      original.latest_dag_runs[0] ? (
        <DagRunInfo
          dataIntervalEnd={original.latest_dag_runs[0].data_interval_end}
          dataIntervalStart={original.latest_dag_runs[0].data_interval_start}
          endDate={original.latest_dag_runs[0].end_date}
          logicalDate={original.latest_dag_runs[0].logical_date}
          startDate={original.latest_dag_runs[0].start_date}
        />
      ) : undefined,
    enableSorting: false,
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

const sortOptions = createListCollection({
  items: [
    { label: "Sort by Dag ID (A-Z)", value: "dag_id" },
    { label: "Sort by Dag ID (Z-A)", value: "-dag_id" },
  ],
});

export const DagsList = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [display, setDisplay] = useLocalStorage<"card" | "table">(
    DAGS_LIST_DISPLAY,
    "card",
  );

  const showPaused = searchParams.get(PAUSED_PARAM);
  const lastDagRunState = searchParams.get(
    LAST_DAG_RUN_STATE_PARAM,
  ) as DagRunState;
  const selectedTags = searchParams.getAll(TAGS_PARAM);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [dagDisplayNamePattern, setDagDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  // TODO: update API to accept multiple orderBy params
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const handleSearchChange = ({
    target: { value },
  }: ChangeEvent<HTMLInputElement>) => {
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

  const { data, error, isFetching, isLoading } = useDags({
    dagDisplayNamePattern: Boolean(dagDisplayNamePattern)
      ? `%${dagDisplayNamePattern}%`
      : undefined,
    lastDagRunState,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    onlyActive: true,
    orderBy,
    paused: showPaused === null ? undefined : showPaused === "true",
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
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          inputProps={{
            defaultValue: dagDisplayNamePattern,
            onChange: handleSearchChange,
          }}
        />
        <DagsFilters />
        <HStack justifyContent="space-between">
          <Heading py={3} size="md">
            {pluralize("Dag", data.total_entries)}
          </Heading>
          {display === "card" ? (
            <Select.Root
              collection={sortOptions}
              data-testid="sort-by-select"
              onValueChange={handleSortChange}
              value={orderBy === undefined ? undefined : [orderBy]}
              width="200px"
            >
              <Select.Trigger>
                <Select.ValueText placeholder="Sort by" />
              </Select.Trigger>
              <Select.Content>
                {sortOptions.items.map((option) => (
                  <Select.Item item={option} key={option.value}>
                    {option.label}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
          ) : (
            false
          )}
        </HStack>
      </VStack>
      <ToggleTableDisplay display={display} setDisplay={setDisplay} />
      <DataTable
        cardDef={cardDef}
        columns={columns}
        data={data.dags}
        displayMode={display}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Dag"
        onStateChange={setTableURLState}
        skeletonCount={display === "card" ? 5 : undefined}
        total={data.total_entries}
      />
    </>
  );
};
