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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { ClearTaskInstanceButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { MarkTaskInstanceAsButton } from "src/components/MarkAs";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAutoRefresh, isStatePending, renderDuration } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

import DeleteTaskInstanceButton from "./DeleteTaskInstanceButton";
import { TaskInstancesFilter } from "./TaskInstancesFilter";

type TaskInstanceRow = { row: { original: TaskInstanceResponse } };

const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  DAG_VERSION: DAG_VERSION_PARAM,
  DURATION_GTE: DURATION_GTE_PARAM,
  DURATION_LTE: DURATION_LTE_PARAM,
  END_DATE: END_DATE_PARAM,
  LOGICAL_DATE_GTE: LOGICAL_DATE_GTE_PARAM,
  LOGICAL_DATE_LTE: LOGICAL_DATE_LTE_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  OPERATOR_NAME_PATTERN: OPERATOR_NAME_PATTERN_PARAM,
  POOL_NAME_PATTERN: POOL_NAME_PATTERN_PARAM,
  QUEUE_NAME_PATTERN: QUEUE_NAME_PATTERN_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  START_DATE: START_DATE_PARAM,
  TASK_STATE: STATE_PARAM,
  TRY_NUMBER: TRY_NUMBER_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const taskInstanceColumns = ({
  dagId,
  runId,
  taskId,
  translate,
}: {
  dagId?: string;
  runId?: string;
  taskId?: string;
  translate: TFunction;
}): Array<ColumnDef<TaskInstanceResponse>> => [
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
  const filteredDagVersion = searchParams.get(DAG_VERSION_PARAM);
  const durationGte = searchParams.get(DURATION_GTE_PARAM);
  const durationLte = searchParams.get(DURATION_LTE_PARAM);
  const logicalDateGte = searchParams.get(LOGICAL_DATE_GTE_PARAM);
  const logicalDateLte = searchParams.get(LOGICAL_DATE_LTE_PARAM);
  const tryNumberFilter = searchParams.get(TRY_NUMBER_PARAM);
  const mapIndexFilter = searchParams.get(MAP_INDEX_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const poolNamePattern = searchParams.get(POOL_NAME_PATTERN_PARAM);
  const queueNamePattern = searchParams.get(QUEUE_NAME_PATTERN_PARAM);
  const operatorNamePattern = searchParams.get(OPERATOR_NAME_PATTERN_PARAM);
  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const filteredRunId = searchParams.get(RUN_ID_PATTERN_PARAM);
  const hasFilteredState = filteredState.length > 0;
  const taskDisplayNamePattern = searchParams.get(NAME_PATTERN_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId: dagId ?? "~",
      dagIdPattern: filteredDagIdPattern ?? undefined,
      dagRunId: runId ?? "~",
      durationGte: durationGte !== null && durationGte !== "" ? Number(durationGte) : undefined,
      durationLte: durationLte !== null && durationLte !== "" ? Number(durationLte) : undefined,
      endDateLte: endDate ?? undefined,
      limit: pagination.pageSize,
      logicalDateGte: logicalDateGte ?? undefined,
      logicalDateLte: logicalDateLte ?? undefined,
      mapIndex: mapIndexFilter !== null && mapIndexFilter !== "" ? [Number(mapIndexFilter)] : undefined,
      offset: pagination.pageIndex * pagination.pageSize,
      operatorNamePattern: operatorNamePattern ?? undefined,
      orderBy,
      poolNamePattern: poolNamePattern ?? undefined,
      queueNamePattern: queueNamePattern ?? undefined,
      runIdPattern: filteredRunId ?? undefined,
      startDateGte: startDate ?? undefined,
      state: hasFilteredState ? filteredState : undefined,
      taskDisplayNamePattern: taskDisplayNamePattern ?? undefined,
      taskGroup: groupId ?? undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
      tryNumber: tryNumberFilter !== null && tryNumberFilter !== "" ? [Number(tryNumberFilter)] : undefined,
      versionNumber:
        filteredDagVersion !== null && filteredDagVersion !== "" ? [Number(filteredDagVersion)] : undefined,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  const columns = useMemo(
    () =>
      taskInstanceColumns({
        dagId,
        runId,
        taskId: Boolean(groupId) ? undefined : taskId,
        translate,
      }),
    [dagId, runId, groupId, taskId, translate],
  );

  return (
    <>
      <TaskInstancesFilter />
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
    </>
  );
};
