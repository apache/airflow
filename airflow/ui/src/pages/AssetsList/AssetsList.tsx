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
import { Box, Heading, Link, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useState } from "react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import { useAssetServiceGetAssets } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { Button, Popover } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { pluralize } from "src/utils";

const columns: Array<ColumnDef<AssetResponse>> = [
  {
    accessorKey: "name",
    header: () => "Name",
  },
  {
    accessorKey: "group",
    enableSorting: false,
    header: () => "Group",
  },
  {
    accessorKey: "consuming_dags",
    cell: ({ row }) =>
      row.original.consuming_dags.length ? (
        <Popover.Root lazyMount unmountOnExit>
          <Popover.Trigger asChild>
            <Button size="sm" variant="outline">
              {pluralize("Dag", row.original.consuming_dags.length)}
            </Button>
          </Popover.Trigger>
          <Popover.Content>
            <Popover.Arrow />
            <Popover.Body>
              <Box>
                {row.original.consuming_dags.map((dag) => (
                  <Link asChild color="fg.info" display="block" key={dag.dag_id} py={2}>
                    <RouterLink to={`/dags/${dag.dag_id}`}>{dag.dag_id}</RouterLink>
                  </Link>
                ))}
              </Box>
            </Popover.Body>
          </Popover.Content>
        </Popover.Root>
      ) : undefined,
    enableSorting: false,
    header: () => "Consuming Dags",
  },
  {
    accessorKey: "producing_tasks",
    cell: ({ row }) =>
      row.original.producing_tasks.length ? (
        <Popover.Root lazyMount unmountOnExit>
          <Popover.Trigger asChild>
            <Button size="sm" variant="outline">
              {pluralize("Task", row.original.producing_tasks.length)}
            </Button>
          </Popover.Trigger>
          <Popover.Content>
            <Popover.Arrow />
            <Popover.Body>
              {row.original.producing_tasks.map((task) => (
                <Link asChild color="fg.info" key={`${task.dag_id}-${task.task_id}`} py={2}>
                  <RouterLink to={`/dags/${task.dag_id}/tasks/${task.task_id}`}>
                    {task.dag_id}.{task.task_id}
                  </RouterLink>
                </Link>
              ))}
            </Popover.Body>
          </Popover.Content>
        </Popover.Root>
      ) : undefined,
    enableSorting: false,
    header: () => "Producing Tasks",
  },
];

const NAME_PATTERN_PARAM = SearchParamsKeys.NAME_PATTERN;

export const AssetsList = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [namePattern, setNamePattern] = useState(searchParams.get(NAME_PATTERN_PARAM) ?? undefined);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const { data, error, isLoading } = useAssetServiceGetAssets({
    limit: pagination.pageSize,
    namePattern,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

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
    setNamePattern(value);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={namePattern ?? ""}
          onChange={handleSearchChange}
          placeHolder="Search Assets"
        />

        <Heading py={3} size="md">
          {pluralize("Asset", data?.total_entries)}
        </Heading>
      </VStack>
      <Box overflow="auto">
        <DataTable
          columns={columns}
          data={data?.assets ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isLoading={isLoading}
          modelName="Asset"
          onStateChange={setTableURLState}
          total={data?.total_entries}
        />
      </Box>
    </>
  );
};
