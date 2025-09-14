/*!
 * Licensed to the Apache Software Foundation (ASF)...
 */
import { Flex, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
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
import { getDuration, useAutoRefresh, isStatePending } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

import DeleteTaskInstanceButton from "./DeleteTaskInstanceButton";
import { RunTaskFilters } from "./RunTaskFilters";

type TaskInstanceRow = { row: { original: TaskInstanceResponse } };

const {
  END_DATE: END_DATE_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  POOL: POOL_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
  TASK_ID_PATTERN: TASK_ID_PATTERN_PARAM,
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
          enableSorting: false,
          header: translate("dagId"),
        },
      ]),
  ...(Boolean(runId)
    ? []
    : [
        {
          accessorKey: "run_after",
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
    accessorKey: "operator_name",
    enableSorting: false,
    header: translate("task.operator"),
  },
  {
    cell: ({ row: { original } }) =>
      Boolean(original.start_date) ? getDuration(original.start_date, original.end_date) : "",
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
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-start_date", "-run_after"];

  const filteredState = searchParams.getAll(STATE_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const pool = searchParams.getAll(POOL_PARAM);
  const hasFilteredState = filteredState.length > 0;
  const hasFilteredPool = pool.length > 0;

  // Read the pill value (name_pattern), but accept legacy task_id_pattern too.
  const taskNamePattern =
    searchParams.get(NAME_PATTERN_PARAM) ?? searchParams.get(TASK_ID_PATTERN_PARAM) ?? undefined;

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId: dagId ?? "~",
      dagRunId: runId ?? "~",
      endDateLte: endDate ?? undefined,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      pool: hasFilteredPool ? pool : undefined,
      startDateGte: startDate ?? undefined,
      state: hasFilteredState ? filteredState : undefined,
      taskDisplayNamePattern: groupId ?? taskNamePattern ?? undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  return (
    <>
      {/* Pill-based FilterBar for Task name + From/To */}
      <RunTaskFilters />

      <DataTable
        columns={taskInstanceColumns({
          dagId,
          runId,
          taskId: Boolean(groupId) ? undefined : taskId,
          translate,
        })}
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
