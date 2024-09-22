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
  Badge,
  Checkbox,
  Heading,
  HStack,
  Select,
  Spinner,
  VStack,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { Select as ReactSelect } from "chakra-react-select";
import { type ChangeEventHandler, useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import { useDagServiceGetDagsPublicDagsGet } from "openapi/queries";
import type { DAGModelResponse } from "openapi/requests/types.gen";

import { DataTable } from "../components/DataTable";
import { useTableURLState } from "../components/DataTable/useTableUrlState";
import { QuickFilterButton } from "../components/QuickFilterButton";
import { SearchBar } from "../components/SearchBar";
import { pluralize } from "../utils/pluralize";

const columns: Array<ColumnDef<DAGModelResponse>> = [
  {
    accessorKey: "dag_id",
    cell: ({ row }) => row.original.dag_display_name,
    header: "DAG",
  },
  {
    accessorKey: "is_paused",
    enableSorting: false,
    header: () => "Is Paused",
  },
  {
    accessorKey: "timetable_description",
    cell: (info) =>
      info.getValue() === "Never, external triggers only"
        ? undefined
        : info.getValue(),
    enableSorting: false,
    header: () => "Schedule",
  },
  {
    accessorKey: "next_dagrun",
    enableSorting: false,
    header: "Next DAG Run",
  },
  {
    accessorKey: "tags",
    cell: ({ row }) => (
      <HStack>
        {row.original.tags.map((tag) => (
          <Badge key={tag.name}>{tag.name}</Badge>
        ))}
      </HStack>
    ),
    enableSorting: false,
    header: () => "Tags",
  },
];

const PAUSED_PARAM = "paused";

// eslint-disable-next-line complexity
export const DagsList = ({ cardView = false }) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM) === "true";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  // TODO: update API to accept multiple orderBy params
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const { data, isLoading } = useDagServiceGetDagsPublicDagsGet({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    onlyActive: true,
    orderBy,
    paused: showPaused,
  });

  const handlePausedChange = useCallback(() => {
    searchParams[showPaused ? "delete" : "set"](PAUSED_PARAM, "true");
    setSearchParams(searchParams);
    setTableURLState({ pagination: { ...pagination, pageIndex: 0 }, sorting });
  }, [
    pagination,
    searchParams,
    setSearchParams,
    setTableURLState,
    showPaused,
    sorting,
  ]);

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
      {isLoading ? <Spinner /> : undefined}
      {!isLoading && Boolean(data?.dags) && (
        <>
          <VStack alignItems="none">
            <SearchBar
              buttonProps={{ isDisabled: true }}
              inputProps={{ isDisabled: true }}
            />
            <HStack justifyContent="space-between">
              <HStack>
                <HStack>
                  <QuickFilterButton isActive>All</QuickFilterButton>
                  <QuickFilterButton isDisabled>Failed</QuickFilterButton>
                  <QuickFilterButton isDisabled>Running</QuickFilterButton>
                  <QuickFilterButton isDisabled>Successful</QuickFilterButton>
                </HStack>
                <Checkbox isChecked={showPaused} onChange={handlePausedChange}>
                  Show Paused DAGs
                </Checkbox>
              </HStack>
              <ReactSelect isDisabled placeholder="Filter by tag" />
            </HStack>
            <HStack justifyContent="space-between">
              <Heading size="md">
                {pluralize("DAG", data?.total_entries)}
              </Heading>
              {cardView ? (
                <Select
                  onChange={handleSortChange}
                  placeholder="Sort by…"
                  value={orderBy}
                  variant="flushed"
                  width="200px"
                >
                  <option value="dag_id">Sort by DAG ID (A-Z)</option>
                  <option value="-dag_id">Sort by DAG ID (Z-A)</option>
                </Select>
              ) : (
                false
              )}
            </HStack>
          </VStack>
          <DataTable
            columns={columns}
            data={data?.dags ?? []}
            initialState={tableURLState}
            onStateChange={setTableURLState}
            total={data?.total_entries}
          />
        </>
      )}
    </>
  );
};
