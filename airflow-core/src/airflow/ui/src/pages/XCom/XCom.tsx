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
import { Box, Heading, Link, HStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useXcomServiceGetXcomEntries } from "openapi/queries";
import type { XComResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getTaskInstanceLinkFromObj } from "src/utils/links";

import { XComEntry } from "./XComEntry";

const { KEY_PATTERN: KEY_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;

const columns = (translate: (key: string) => string): Array<ColumnDef<XComResponse>> => [
  {
    accessorKey: "key",
    enableSorting: false,
    header: translate("xcom.columns.key"),
  },
  {
    accessorKey: "dag_id",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}`}>{original.dag_display_name}</RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("xcom.columns.dag"),
  },
  {
    accessorKey: "run_id",
    cell: ({ row: { original } }: { row: { original: XComResponse } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.run_id}`}>
          <TruncatedText text={original.run_id} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:runId"),
  },
  {
    accessorKey: "task_id",
    cell: ({ row: { original } }: { row: { original: XComResponse } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink
          to={getTaskInstanceLinkFromObj({
            dagId: original.dag_id,
            dagRunId: original.run_id,
            mapIndex: original.map_index,
            taskId: original.task_id,
          })}
        >
          <TruncatedText text={original.task_id} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:taskId"),
  },
  {
    accessorKey: "map_index",
    enableSorting: false,
    header: translate("common:mapIndex"),
  },
  {
    cell: ({ row: { original } }) => (
      <XComEntry
        dagId={original.dag_id}
        mapIndex={original.map_index}
        runId={original.run_id}
        taskId={original.task_id}
        xcomKey={original.key}
      />
    ),
    enableSorting: false,
    header: translate("xcom.columns.value"),
  },
];

export const XCom = () => {
  const { dagId = "~", mapIndex = "-1", runId = "~", taskId = "~" } = useParams();
  const { t: translate } = useTranslation(["browse", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [searchParams, setSearchParams] = useSearchParams();

  const filteredKey = searchParams.get(KEY_PATTERN_PARAM);
  const filteredDagId = searchParams.get("dag_id");
  const filteredRunId = searchParams.get("run_id");
  const filteredTaskId = searchParams.get("task_id");

  const { data, error, isFetching, isLoading } = useXcomServiceGetXcomEntries(
    {
      dagId: filteredDagId ?? dagId,
      dagRunId: filteredRunId ?? runId,
      limit: pagination.pageSize,
      mapIndex: mapIndex === "-1" ? undefined : parseInt(mapIndex, 10),
      offset: pagination.pageIndex * pagination.pageSize,
      taskId: filteredTaskId ?? taskId,
      xcomKeyPattern: filteredKey ?? undefined,
    },
    undefined,
    { enabled: !isNaN(pagination.pageSize) },
  );

  const handleKeyFilterChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete(KEY_PATTERN_PARAM);
      } else {
        searchParams.set(KEY_PATTERN_PARAM, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleDagIdFilterChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete("dag_id");
      } else {
        searchParams.set("dag_id", value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleRunIdFilterChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete("run_id");
      } else {
        searchParams.set("run_id", value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleTaskIdFilterChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete("task_id");
      } else {
        searchParams.set("task_id", value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  return (
    <Box>
      {dagId === "~" && runId === "~" && taskId === "~" ? (
        <Heading size="md">{translate("xcom.title")}</Heading>
      ) : undefined}

      <HStack flexWrap="wrap" gap={4} paddingY="4px">
        <Box minW="200px">
          <SearchBar
            defaultValue={filteredKey ?? ""}
            hideAdvanced
            hotkeyDisabled={false}
            onChange={handleKeyFilterChange}
            placeHolder={translate("xcom.filters.keyFilter")}
          />
        </Box>
        <Box minW="200px">
          <SearchBar
            defaultValue={filteredDagId ?? ""}
            hideAdvanced
            hotkeyDisabled={true}
            onChange={handleDagIdFilterChange}
            placeHolder={translate("xcom.filters.dagFilter")}
          />
        </Box>
        <Box minW="200px">
          <SearchBar
            defaultValue={filteredRunId ?? ""}
            hideAdvanced
            hotkeyDisabled={true}
            onChange={handleRunIdFilterChange}
            placeHolder={translate("xcom.filters.runIdFilter")}
          />
        </Box>
        <Box minW="200px">
          <SearchBar
            defaultValue={filteredTaskId ?? ""}
            hideAdvanced
            hotkeyDisabled={true}
            onChange={handleTaskIdFilterChange}
            placeHolder={translate("xcom.filters.taskIdFilter")}
          />
        </Box>
      </HStack>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns(translate)}
        data={data ? data.xcom_entries : []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("xcom.title")}
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
