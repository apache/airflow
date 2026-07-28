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
import { Badge, Flex, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import {
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskStateStoreServiceListTaskStateStore,
} from "openapi/queries";
import type { TaskStateStoreResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StoreValueCell } from "src/components/StoreValueCell";
import Time from "src/components/Time";
import { isStatePending, useAutoRefresh } from "src/utils";

import { AddTaskStateStoreButton } from "./AddTaskStateStoreButton";
import { ClearAllTaskStateStoreButton } from "./ClearAllTaskStateStoreButton";
import { DeleteTaskStateStoreButton } from "./DeleteTaskStateStoreButton";
import { EditTaskStateStoreButton } from "./EditTaskStateStoreButton";

type ColumnsProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly runId: string;
  readonly taskId: string;
  readonly translate: (key: string) => string;
};

const getColumns = ({
  dagId,
  mapIndex,
  runId,
  taskId,
  translate,
}: ColumnsProps): Array<ColumnDef<TaskStateStoreResponse>> => [
  {
    accessorKey: "key",
    cell: ({ row: { original } }) => <Text>{original.key}</Text>,
    header: translate("common:key"),
  },
  {
    accessorKey: "value",
    cell: ({ row: { original } }) => <StoreValueCell value={original.value} />,
    enableSorting: false,
    header: translate("common:value"),
  },
  {
    accessorKey: "updated_at",
    cell: ({ row: { original } }) => <Time datetime={original.updated_at} />,
    header: translate("common:table.updatedAt"),
  },
  {
    accessorKey: "expires_at",
    cell: ({ row: { original } }) =>
      Boolean(original.expires_at) ? (
        <Time datetime={original.expires_at} />
      ) : (
        <Badge colorPalette="gray" variant="subtle">
          {translate("taskStateStore.expiresAt.never")}
        </Badge>
      ),
    header: translate("taskStateStore.expiresAt.column"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditTaskStateStoreButton
          dagId={dagId}
          mapIndex={mapIndex}
          runId={runId}
          storeKey={original.key}
          taskId={taskId}
        />
        <DeleteTaskStateStoreButton
          dagId={dagId}
          mapIndex={mapIndex}
          runId={runId}
          storeKey={original.key}
          taskId={taskId}
        />
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const TaskStateStore = () => {
  const { dagId = "", mapIndex: rawMapIndex = "-1", runId = "", taskId = "" } = useParams();
  const mapIndex = parseInt(rawMapIndex, 10);
  const refetchInterval = useAutoRefresh({ dagId });

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance({
    dagId,
    dagRunId: runId,
    mapIndex,
    taskId,
  });

  const { t: translate } = useTranslation(["dag", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useTaskStateStoreServiceListTaskStateStore(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      mapIndex,
      offset: pagination.pageIndex * pagination.pageSize,
      taskId,
    },
    undefined,
    { refetchInterval: isStatePending(taskInstance?.state) ? refetchInterval : false },
  );

  const columns = getColumns({ dagId, mapIndex, runId, taskId, translate });

  return (
    <>
      <Flex gap={2} justifyContent="flex-end" mb={2}>
        <AddTaskStateStoreButton dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
        {(data?.total_entries ?? 0) > 0 ? (
          <ClearAllTaskStateStoreButton dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
        ) : undefined}
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.task_state_store ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="dag:taskStateStore.title"
        noRowsMessage={translate("taskStateStore.emptyStore")}
        onStateChange={setTableURLState}
        showRowCountHeading={false}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
