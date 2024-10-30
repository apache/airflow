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
  Select,
  Skeleton,
  VStack,
  Link,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import {
  type ChangeEvent,
  type ChangeEventHandler,
  useCallback,
  useState,
} from "react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type {
  DagRunState,
  DAGWithLatestDagRunsResponse,
} from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ToggleTableDisplay } from "src/components/DataTable/ToggleTableDisplay";
import type { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";
import {
  SearchParamsKeys,
  type SearchParamsKeysType,
} from "src/constants/searchParams";
import { useDags } from "src/queries/useDags";
import { pluralize } from "src/utils";

import { DagCard } from "./DagCard";
import { DagTags } from "./DagTags";
import { DagsFilters } from "./DagsFilters";
import { LatestRun } from "./LatestRun";
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
      <Link
        as={RouterLink}
        color="blue.contrast"
        fontWeight="bold"
        to={`/dags/${original.dag_id}`}
      >
        {original.dag_display_name}
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
        <Time datetime={original.next_dagrun} />
      ) : undefined,
    enableSorting: false,
    header: "Next Dag Run",
  },
  {
    accessorKey: "latest_dag_runs",
    cell: ({ row: { original } }) => (
      <LatestRun latestRun={original.latest_dag_runs[0]} />
    ),
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

  const handleSortChange = useCallback<ChangeEventHandler<HTMLSelectElement>>(
    ({ currentTarget: { value } }) => {
      setTableURLState({
        pagination,
        sorting: value
          ? [{ desc: value.startsWith("-"), id: value.replace("-", "") }]
          : [],
      });
    },
    [pagination, setTableURLState],
  );

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ isDisabled: true }}
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
            <Select
              data-testid="sort-by-select"
              onChange={handleSortChange}
              placeholder="Sort by…"
              value={orderBy}
              variant="flushed"
              width="200px"
            >
              <option value="dag_id">Sort by Dag ID (A-Z)</option>
              <option value="-dag_id">Sort by Dag ID (Z-A)</option>
            </Select>
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
