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
import { Flex, Link } from "@chakra-ui/react";
import { Heading } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useHumanInTheLoopServiceGetHitlDetails } from "openapi/queries";
import type { HITLDetailWithTaskInstance } from "openapi/requests/types.gen";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { Dialog } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getDuration, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

import HITLResponseButton from "./HITLResponseButton";
import { HITLResponseForm } from "./HITLResponseForm";
import { HITLTaskInstancesFilter } from "./HITLTaskInstancesFilter";
import { useHITLResponseState } from "./useHITLResponseState";

type TaskInstanceRow = { row: { original: HITLDetailWithTaskInstance } };

const {
  END_DATE: END_DATE_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  POOL: POOL_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
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
}): Array<ColumnDef<HITLDetailWithTaskInstance>> => [
  {
    accessorKey: "actions",
    cell: ({ row }) => (
      <Flex justifyContent="end">
        <HITLResponseButton taskInstance={row.original.taskInstance} withText={false} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
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
          // If we don't show the taskId column, make the dag run a link to the task instance
          cell: ({ row: { original } }: TaskInstanceRow) =>
            Boolean(taskId) ? (
              <Link asChild color="fg.info" fontWeight="bold">
                <RouterLink to={getTaskInstanceLink(original.taskInstance)}>
                  <Time datetime={original.taskInstance.run_after} />
                </RouterLink>
              </Link>
            ) : (
              <Time datetime={original.taskInstance.run_after} />
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
              <RouterLink to={getTaskInstanceLink(original.taskInstance)}>
                <TruncatedText text={original.taskInstance.task_display_name} />
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
        original: {
          taskInstance: { state },
        },
      },
    }) => <StateBadge state={state}>{translate(`common:states.${state}`)}</StateBadge>,
    header: () => translate("state"),
  },
  {
    accessorKey: "start_date",
    cell: ({ row: { original } }) =>
      Boolean(taskId) && Boolean(runId) ? (
        <Link asChild color="fg.info" fontWeight="bold">
          <RouterLink to={getTaskInstanceLink(original.taskInstance)}>
            <Time datetime={original.taskInstance.start_date} />
          </RouterLink>
        </Link>
      ) : (
        <Time datetime={original.taskInstance.start_date} />
      ),
    header: translate("startDate"),
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.taskInstance.end_date} />,
    header: translate("endDate"),
  },
  {
    accessorKey: "try_number",
    enableSorting: false,
    header: translate("tryNumber"),
  },
  {
    accessorKey: "taskInstance.operator",
    enableSorting: false,
    header: translate("task.operator"),
  },
  {
    cell: ({ row: { original } }) =>
      Boolean(original.taskInstance.start_date)
        ? getDuration(original.taskInstance.start_date, original.taskInstance.end_date)
        : "",
    header: translate("duration"),
  },
  {
    accessorKey: "dag_version",
    cell: ({ row: { original } }) => <DagVersion version={original.taskInstance.dag_version} />,
    enableSorting: false,
    header: translate("taskInstance.dagVersion"),
  },
];

export const HITLTaskInstances = () => {
  const { t: translate } = useTranslation();
  const { dagId, groupId, runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-start_date";

  const filteredState = searchParams.getAll(STATE_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const pool = searchParams.getAll(POOL_PARAM);
  const hasFilteredState = filteredState.length > 0;
  const hasFilteredPool = pool.length > 0;
  const {
    isOpen: isHITLResponseFormOpen,
    onClose: onHITLResponseFormClose,
    taskInstance: hitlTaskInstance,
  } = useHITLResponseState();

  const [taskDisplayNamePattern, setTaskDisplayNamePattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useHumanInTheLoopServiceGetHitlDetails(undefined, {
    enabled: !isNaN(pagination.pageSize),
    refetchInterval: (query) =>
      query.state.data?.hitl_details.some((ti) => ti.taskInstance.state === "deferred")
        ? refetchInterval
        : false,
  });

  console.log(data);

  const filteredData = data?.hitl_details.filter((ti) => {
    if (Boolean(dagId) && Boolean(runId)) {
      return ti.taskInstance.dag_id === dagId && ti.taskInstance.dag_run_id === runId;
    }

    if (Boolean(dagId)) {
      return ti.taskInstance.dag_id === dagId;
    }

    return true;
  });

  return (
    <>
      <HITLTaskInstancesFilter
        setTaskDisplayNamePattern={setTaskDisplayNamePattern}
        taskDisplayNamePattern={taskDisplayNamePattern}
      />
      <DataTable
        columns={taskInstanceColumns({
          dagId,
          runId,
          taskId: Boolean(groupId) ? undefined : taskId,
          translate,
        })}
        data={filteredData ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName={translate("common:taskInstance_other")}
        onStateChange={setTableURLState}
        total={filteredData?.length}
      />
      <Dialog.Root
        lazyMount
        onOpenChange={onHITLResponseFormClose}
        open={isHITLResponseFormOpen}
        size="xl"
        unmountOnExit
      >
        <Dialog.Content backdrop>
          <Dialog.Header>
            <Heading>
              {translate("hitl:response.title")} - {hitlTaskInstance?.task_id}
            </Heading>
          </Dialog.Header>
          <Dialog.CloseTrigger />
          <Dialog.Body>
            <HITLResponseForm />
          </Dialog.Body>
        </Dialog.Content>
      </Dialog.Root>
    </>
  );
};
