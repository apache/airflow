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
import { Heading, Link, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetHitlDetails } from "openapi/queries";
import type { HITLDetail } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAutoRefresh } from "src/utils";
import { getHITLState } from "src/utils/hitl";
import { getTaskInstanceLink } from "src/utils/links";

import { HITLFilters } from "./HITLFilters";

type TaskInstanceRow = { row: { original: HITLDetail } };

const {
  DAG_DISPLAY_NAME_PATTERN,
  OFFSET: OFFSET_PARAM,
  RESPONSE_RECEIVED: RESPONSE_RECEIVED_PARAM,
  TASK_ID_PATTERN,
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
}): Array<ColumnDef<HITLDetail>> => [
  {
    accessorKey: "task_instance_state",
    cell: ({ row: { original } }: TaskInstanceRow) => (
      <StateBadge state={original.task_instance.state}>{getHITLState(translate, original)}</StateBadge>
    ),
    header: translate("requiredActionState"),
  },
  {
    accessorKey: "subject",
    cell: ({ row: { original } }: TaskInstanceRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`${getTaskInstanceLink(original.task_instance)}/required_actions`}>
          <TruncatedText text={original.subject} />
        </RouterLink>
      </Link>
    ),
    header: translate("subject"),
  },
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "task_instance.dag_id",
          enableSorting: false,
          header: translate("common:dagId"),
        },
      ]),
  ...(Boolean(runId)
    ? []
    : [
        {
          accessorKey: "run_after",
          cell: ({ row: { original } }: TaskInstanceRow) => (
            <Time datetime={original.task_instance.run_after} />
          ),
          header: translate("common:dagRun.runAfter"),
        },
      ]),
  ...(Boolean(taskId)
    ? []
    : [
        {
          accessorKey: "task_display_name",
          cell: ({ row: { original } }: TaskInstanceRow) => (
            <TruncatedText text={original.task_instance.task_display_name} />
          ),
          enableSorting: false,
          header: translate("common:taskId"),
        },
      ]),
  {
    accessorKey: "rendered_map_index",
    header: translate("common:mapIndex"),
  },
  {
    accessorKey: "responded_at",
    cell: ({ row: { original } }) => <Time datetime={original.responded_at} />,
    header: translate("response.received"),
  },
];

export const HITLTaskInstances = () => {
  const { t: translate } = useTranslation("hitl");
  const { dagId, runId, taskId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const responseReceived = searchParams.get(RESPONSE_RECEIVED_PARAM);

  const baseRefetchInterval = useAutoRefresh({});

  const dagIdPattern = searchParams.get(DAG_DISPLAY_NAME_PATTERN) ?? undefined;
  const taskIdPattern = searchParams.get(TASK_ID_PATTERN) ?? undefined;
  const filterResponseReceived = searchParams.get(RESPONSE_RECEIVED_PARAM) ?? undefined;

  // Use the filter value if available, otherwise fall back to the old responseReceived param
  const effectiveResponseReceived = filterResponseReceived ?? responseReceived;

  const { data, error, isLoading } = useTaskInstanceServiceGetHitlDetails(
    {
      dagId: dagId ?? "~",
      dagIdPattern,
      dagRunId: runId ?? "~",
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy: sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : [],
      responseReceived:
        Boolean(effectiveResponseReceived) && effectiveResponseReceived !== "all"
          ? effectiveResponseReceived === "true"
          : undefined,
      state: effectiveResponseReceived === "false" ? ["deferred"] : undefined,
      taskId,
      taskIdPattern,
    },
    undefined,
    {
      // Only continue auto-refetching when filtering for unreceived responses
      // and at least one TaskInstance is still deferred without a response.
      refetchInterval: (query) => {
        const hasDeferredWithoutResponse = Boolean(
          query.state.data?.hitl_details.some(
            (detail: HITLDetail) =>
              detail.responded_at === undefined && detail.task_instance.state === "deferred",
          ),
        );

        return hasDeferredWithoutResponse ? baseRefetchInterval : false;
      },
    },
  );

  const handleResponseChange = useCallback(() => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    searchParams.delete(OFFSET_PARAM);
    setSearchParams(searchParams);
  }, [pagination, searchParams, setSearchParams, setTableURLState, sorting]);

  return (
    <VStack align="start">
      {!Boolean(dagId) && !Boolean(runId) && !Boolean(taskId) ? (
        <Heading size="md">
          {data?.total_entries} {translate("requiredAction", { count: data?.total_entries })}
        </Heading>
      ) : undefined}
      <HITLFilters onResponseChange={handleResponseChange} />
      <DataTable
        columns={taskInstanceColumns({
          dagId,
          runId,
          taskId,
          translate,
        })}
        data={data?.hitl_details ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName={translate("requiredAction_other")}
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </VStack>
  );
};
