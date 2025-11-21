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
import { Box, Heading, Link, Flex, useDisclosure } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useXcomServiceGetXcomEntries } from "openapi/queries";
import type { XComResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ExpandCollapseButtons } from "src/components/ExpandCollapseButtons";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getTaskInstanceLink } from "src/utils/links";

import { XComEntry } from "./XComEntry";
import { XComFilters } from "./XComFilters";

const {
  DAG_DISPLAY_NAME_PATTERN: DAG_DISPLAY_NAME_PATTERN_PARAM,
  KEY_PATTERN: KEY_PATTERN_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  TASK_ID_PATTERN: TASK_ID_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const columns = (translate: (key: string) => string, open: boolean): Array<ColumnDef<XComResponse>> => [
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
    accessorKey: "task_display_name",
    cell: ({ row: { original } }: { row: { original: XComResponse } }) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink
          to={getTaskInstanceLink({
            dagId: original.dag_id,
            dagRunId: original.run_id,
            mapIndex: original.map_index,
            taskId: original.task_id,
          })}
        >
          <TruncatedText text={original.task_display_name} />
        </RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("common:task_one"),
  },
  {
    accessorKey: "map_index",
    enableSorting: false,
    header: translate("common:mapIndex"),
  },
  {
    accessorKey: "timestamp",
    cell: ({ row: { original } }) => <Time datetime={original.timestamp} />,
    enableSorting: false,
    header: translate("dashboard:timestamp"),
  },
  {
    cell: ({ row: { original } }) => (
      <XComEntry
        dagId={original.dag_id}
        mapIndex={original.map_index}
        open={open}
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
  const { pagination } = tableURLState;
  const [searchParams] = useSearchParams();
  const { onClose, onOpen, open } = useDisclosure();

  const filteredKey = searchParams.get(KEY_PATTERN_PARAM);
  const filteredDagDisplayName = searchParams.get(DAG_DISPLAY_NAME_PATTERN_PARAM);
  const filteredMapIndex = searchParams.get(MAP_INDEX_PARAM);
  const filteredRunId = searchParams.get(RUN_ID_PATTERN_PARAM);
  const filteredTaskId = searchParams.get(TASK_ID_PATTERN_PARAM);

  const { LOGICAL_DATE_GTE, LOGICAL_DATE_LTE, RUN_AFTER_GTE, RUN_AFTER_LTE } = SearchParamsKeys;
  const logicalDateGte = searchParams.get(LOGICAL_DATE_GTE);
  const logicalDateLte = searchParams.get(LOGICAL_DATE_LTE);
  const runAfterGte = searchParams.get(RUN_AFTER_GTE);
  const runAfterLte = searchParams.get(RUN_AFTER_LTE);

  const apiParams = {
    dagDisplayNamePattern: filteredDagDisplayName ?? undefined,
    dagId,
    dagRunId: runId,
    limit: pagination.pageSize,
    logicalDateGte: logicalDateGte ?? undefined,
    logicalDateLte: logicalDateLte ?? undefined,
    mapIndex:
      filteredMapIndex !== null && filteredMapIndex !== ""
        ? parseInt(filteredMapIndex, 10)
        : mapIndex === "-1"
          ? undefined
          : parseInt(mapIndex, 10),
    offset: pagination.pageIndex * pagination.pageSize,
    runAfterGte: runAfterGte ?? undefined,
    runAfterLte: runAfterLte ?? undefined,
    runIdPattern: filteredRunId ?? undefined,
    taskId,
    taskIdPattern: filteredTaskId ?? undefined,
    xcomKeyPattern: filteredKey ?? undefined,
  };

  const { data, error, isFetching, isLoading } = useXcomServiceGetXcomEntries(apiParams, undefined);

  const memoizedColumns = useMemo(() => columns(translate, open), [translate, open]);

  return (
    <Box>
      {dagId === "~" && runId === "~" && taskId === "~" ? (
        <Heading size="md">{translate("xcom.title")}</Heading>
      ) : undefined}

      <Flex alignItems="center" justifyContent="space-between">
        <XComFilters />
        <ExpandCollapseButtons
          collapseLabel={translate("auditLog.actions.collapseAllExtra")}
          expandLabel={translate("auditLog.actions.expandAllExtra")}
          onCollapse={onClose}
          onExpand={onOpen}
        />
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={memoizedColumns}
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
