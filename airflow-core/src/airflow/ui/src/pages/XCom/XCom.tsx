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
import { Box, Heading, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useXcomServiceGetXcomEntries } from "openapi/queries";
import type { XComResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { TruncatedText } from "src/components/TruncatedText";
import { getTaskInstanceLinkFromObj } from "src/utils/links";

import { XComEntry } from "./XComEntry";

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
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useXcomServiceGetXcomEntries(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      mapIndex: mapIndex === "-1" ? undefined : parseInt(mapIndex, 10),
      offset: pagination.pageIndex * pagination.pageSize,
      taskId,
    },
    undefined,
    { enabled: !isNaN(pagination.pageSize) },
  );

  return (
    <Box>
      {dagId === "~" && runId === "~" && taskId === "~" ? (
        <Heading size="md">{translate("xcom.title")}</Heading>
      ) : undefined}
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
