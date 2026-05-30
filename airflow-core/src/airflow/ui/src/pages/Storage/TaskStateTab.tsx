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

import type { TaskStateResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { useListTaskStates } from "src/queries/useTaskState";

import AddTaskStateButton from "./AddTaskStateButton";
import ClearAllTaskStateButton from "./ClearAllTaskStateButton";
import DeleteTaskStateButton from "./DeleteTaskStateButton";
import EditTaskStateButton from "./EditTaskStateButton";

type TaskStateTabProps = {
  readonly dagId: string;
  readonly mapIndex: number;
  readonly refetchInterval?: number | false;
  readonly runId: string;
  readonly taskId: string;
};

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
}: ColumnsProps): Array<ColumnDef<TaskStateResponse>> => [
  {
    accessorKey: "key",
    cell: ({ row: { original } }) => <Text>{original.key}</Text>,
    header: translate("key"),
  },
  {
    accessorKey: "value",
    cell: ({ row: { original } }) => {
      let parsed: unknown;

      try {
        parsed = JSON.parse(original.value);
      } catch {
        // not JSON — render as plain text
      }
      const isJsonObject = parsed !== null && parsed !== undefined && typeof parsed === "object";

      return isJsonObject ? (
        <RenderedJsonField collapsed content={parsed as object} enableClipboard={false} />
      ) : (
        <TruncatedText text={original.value} />
      );
    },
    enableSorting: false,
    header: translate("value"),
  },
  {
    accessorKey: "updated_at",
    cell: ({ row: { original } }) => <Time datetime={original.updated_at} />,
    header: translate("taskState.columns.updatedAt"),
  },
  {
    accessorKey: "expires_at",
    cell: ({ row: { original } }) =>
      Boolean(original.expires_at) ? (
        <Time datetime={original.expires_at} />
      ) : (
        <Badge colorPalette="gray" variant="subtle">
          {translate("taskState.columns.noExpiry")}
        </Badge>
      ),
    header: translate("taskState.columns.expiresAt"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditTaskStateButton
          dagId={dagId}
          mapIndex={mapIndex}
          runId={runId}
          stateKey={original.key}
          taskId={taskId}
        />
        <DeleteTaskStateButton
          dagId={dagId}
          mapIndex={mapIndex}
          runId={runId}
          stateKey={original.key}
          taskId={taskId}
        />
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const TaskStateTab = ({ dagId, mapIndex, refetchInterval, runId, taskId }: TaskStateTabProps) => {
  const { t: translate } = useTranslation("browse");
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useListTaskStates(
    {
      dagId,
      dagRunId: runId,
      limit: pagination.pageSize,
      mapIndex,
      offset: pagination.pageIndex * pagination.pageSize,
      taskId,
    },
    undefined,
    { refetchInterval },
  );

  const columns = getColumns({ dagId, mapIndex, runId, taskId, translate });

  return (
    <>
      <Flex gap={2} justifyContent="flex-end" mb={2}>
        <AddTaskStateButton dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
        {(data?.total_entries ?? 0) > 0 ? (
          <ClearAllTaskStateButton dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
        ) : undefined}
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.task_states ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="browse:taskState.title"
        noRowsMessage={translate("taskState.emptyState")}
        onStateChange={setTableURLState}
        showRowCountHeading={false}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
