/* eslint-disable max-lines */

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

/* eslint-disable max-lines */
import { Flex, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { type GetColumnsParams, useRowSelection } from "src/components/DataTable/useRowSelection";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { ActionBar } from "src/components/ui/ActionBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { Tooltip } from "src/components/ui/Tooltip";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAutoRefresh, isStatePending, renderDuration } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

import DeleteTaskInstanceButton from "./DeleteTaskInstanceButton";
import DeleteTaskInstancesButton from "./DeleteTaskInstancesButton";
import { TaskInstancesFilter } from "./TaskInstancesFilter";

const SEPARATOR = "SEPARATOR";

type TaskInstanceRow = { row: { original: TaskInstanceResponse } };

const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  END_DATE: END_DATE_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  POOL: POOL_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const getColumns = ({
  allRowsSelected,
  dagId,
  onRowSelect,
  onSelectAll,
  runId,
  selectedRows,
  taskId,
  translate,
}: {
  dagId?: string;
  runId?: string;
  taskId?: string;
  translate: TFunction;
} & GetColumnsParams): Array<ColumnDef<TaskInstanceResponse>> => [
  {
    accessorKey: "select",
    cell: ({ row }: TaskInstanceRow) => (
      <Checkbox
        borderWidth={1}
        checked={selectedRows.get(
          `${row.original.dag_run_id}${SEPARATOR}${row.original.task_id}${SEPARATOR}${row.original.map_index}`,
        )}
        colorPalette="blue"
        onCheckedChange={(event) =>
          onRowSelect(
            `${row.original.dag_run_id}${SEPARATOR}${row.original.task_id}${SEPARATOR}${row.original.map_index}`,
            Boolean(event.checked),
          )
        }
      />
    ),
    enableSorting: false,
    header: () => (
      <Checkbox
        borderWidth={1}
        checked={allRowsSelected}
        colorPalette="blue"
        onCheckedChange={(event) => onSelectAll(Boolean(event.checked))}
      />
    ),
    meta: {
      skeletonWidth: 10,
    },
  },
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_display_name",
          cell: ({ row: { original } }: TaskInstanceRow) => (
            <Link asChild color="fg.info">
              <RouterLink to={`/dags/${original.dag_id}`}>
                <TruncatedText text={original.dag_display_name} />
              </RouterLink>
            </Link>
          ),
          enableSorting: false,
          header: translate("dagId"),
        },
      ]),
  ...(Boolean(runId)
    ? []
    : [
        {
          accessorKey: "run_after",
          // If we don't show the taskId column, make the dag run a link to the task instance
          cell: ({ row: { original } }: TaskInstanceRow) =>
            Boolean(taskId) ? (
              <Link asChild color="fg.info" fontWeight="bold">
                <RouterLink to={getTaskInstanceLink(original)}>
                  <Time datetime={original.run_after} />
                </RouterLink>
              </Link>
            ) : (
              <Time datetime={original.run_after} />
            ),
          header: translate("dagRun_one"),
        },
      ]),
  ...(Boolean(taskId)
    ? []
    : [
        {
          accessorKey: "task_display_name",
          cell: ({ row: { original } }: TaskInstanceRow) => (
            <Link asChild color="fg.info" fontWeight="bold">
              <RouterLink to={getTaskInstanceLink(original)}>
                <TruncatedText text={original.task_display_name} />
              </RouterLink>
            </Link>
          ),
          enableSorting: false,
          header: translate("taskId"),
        },
      ]),
  {
    accessorKey: "rendered_map_index",
    header: translate("mapIndex"),
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => (
      <StateBadge state={state}>
        {state ? translate(`common:states.${state}`) : translate("common:states.no_status")}
      </StateBadge>
    ),
    header: () => translate("state"),
  },
  {
    accessorKey: "start_date",
    cell: ({ row: { original } }) =>
      Boolean(taskId) && Boolean(runId) ? (
        <Link asChild color="fg.info" fontWeight="bold">
          <RouterLink to={getTaskInstanceLink(original)}>
            <Time datetime={original.start_date} />
          </RouterLink>
        </Link>
      ) : (
        <Time datetime={original.start_date} />
      ),
    header: translate("startDate"),
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: translate("endDate"),
  },
  {
    accessorKey: "try_number",
    enableSorting: false,
    header: translate("tryNumber"),
  },
  {
    accessorKey: "pool",
    enableSorting: false,
    header: translate("taskInstance.pool"),
  },
  {
    accessorKey: "queue",
    enableSorting: false,
    header: translate("taskInstance.queue"),
  },
  {
    accessorKey: "executor",
    enableSorting: false,
    header: translate("taskInstance.executor"),
  },
  {
    accessorKey: "hostname",
    enableSorting: false,
    header: translate("taskInstance.hostname"),
  },
  {
    accessorKey: "operator_name",
    enableSorting: false,
    header: translate("task.operator"),
  },
  {
    accessorKey: "duration",
    cell: ({ row: { original } }) => renderDuration(original.duration),
    header: translate("duration"),
  },
  {
    accessorKey: "dag_version",
    cell: ({ row: { original } }) => <DagVersion version={original.dag_version} />,
    enableSorting: false,
    header: translate("taskInstance.dagVersion"),
  },
  {
    accessorKey: "actions",
    cell: ({ row }) => (
      <Flex justifyContent="end">
        <ClearTaskInstanceButton taskInstance={row.original} withText={false} />
        <MarkTaskInstanceAsButton taskInstance={row.original} withText={false} />
        <DeleteTaskInstanceButton taskInstance={row.original} withText={false} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const TaskInstances = () => {
  const { t: translate } = useTranslation();
  const { dagId, groupId, runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState({
    columnVisibility: {
      dag_version: false,
      end_date: false,
      executor: false,
      hostname: false,
      pool: false,
      queue: false,
    },
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-start_date", "-run_after"];

  const filteredState = searchParams.getAll(STATE_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const pool = searchParams.getAll(POOL_PARAM);
  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const hasFilteredState = filteredState.length > 0;
  const hasFilteredPool = pool.length > 0;

  const [taskDisplayNamePattern, setTaskDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId: dagId ?? "~",
      dagIdPattern: filteredDagIdPattern ?? undefined,
      dagRunId: runId ?? "~",
      endDateLte: endDate ?? undefined,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      pool: hasFilteredPool ? pool : undefined,
      startDateGte: startDate ?? undefined,
      state: hasFilteredState ? filteredState : undefined,
      taskDisplayNamePattern: groupId ?? taskDisplayNamePattern ?? undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  const { allRowsSelected, clearSelections, handleRowSelect, handleSelectAll, selectedRows } =
    useRowSelection({
      data: data?.task_instances,
      getKey: (taskInstance) =>
        `${taskInstance.dag_run_id}${SEPARATOR}${taskInstance.task_id}${SEPARATOR}${taskInstance.map_index}`,
    });

  const columns = useMemo(
    () =>
      getColumns({
        allRowsSelected,
        dagId,
        onRowSelect: handleRowSelect,
        onSelectAll: handleSelectAll,
        runId,
        selectedRows,
        taskId: Boolean(groupId) ? undefined : taskId,
        translate,
      }),
    [
      allRowsSelected,
      dagId,
      groupId,
      handleRowSelect,
      handleSelectAll,
      runId,
      selectedRows,
      taskId,
      translate,
    ],
  );

  return (
    <>
      <TaskInstancesFilter
        setTaskDisplayNamePattern={setTaskDisplayNamePattern}
        taskDisplayNamePattern={taskDisplayNamePattern}
      />
      <DataTable
        columns={columns}
        data={data?.task_instances ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName={translate("common:taskInstance_other")}
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
      <ActionBar.Root closeOnInteractOutside={false} open={Boolean(selectedRows.size)}>
        <ActionBar.Content>
          <ActionBar.SelectionTrigger>
            {selectedRows.size} {translate("common:selected")}
          </ActionBar.SelectionTrigger>
          <ActionBar.Separator />
          <Tooltip
            content={`${translate("common:modal.delete.button")} ${translate(
              `common:${selectedRows.size > 1 ? "taskInstance_other" : "taskInstance_one"}`,
            )}`}
            disabled={selectedRows.size === 0}
          >
            <DeleteTaskInstancesButton
              clearSelections={clearSelections}
              dagId={dagId ?? ""}
              dagRunId={runId ?? ""}
              deleteKeys={
                [...selectedRows.keys()]
                  .map((id) => {
                    const [dagRunId, currentTaskId, mapIndex] = id.split(SEPARATOR);

                    return data?.task_instances.find(
                      (ti) =>
                        ti.dag_run_id === dagRunId &&
                        ti.task_id === currentTaskId &&
                        ti.map_index === Number(mapIndex ?? -1),
                    );
                  })
                  .filter(Boolean) as Array<TaskInstanceResponse>
              }
            />
          </Tooltip>
          <ActionBar.CloseTrigger onClick={clearSelections} />
        </ActionBar.Content>
      </ActionBar.Root>
    </>
  );
};
