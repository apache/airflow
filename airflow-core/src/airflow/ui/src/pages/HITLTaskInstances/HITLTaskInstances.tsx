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
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useHumanInTheLoopServiceGetHitlDetails } from "openapi/queries";
import type { HITLDetail } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useAutoRefresh } from "src/utils";
import { getHITLState } from "src/utils/hitl";
import { getTaskInstanceLink } from "src/utils/links";

type TaskInstanceRow = { row: { original: HITLDetail } };

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
    accessorKey: "task_instance.operator",
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
          // If we don't show the taskId column, make the dag run a link to the task instance
          cell: ({ row: { original } }: TaskInstanceRow) =>
            Boolean(taskId) ? (
              <Link asChild color="fg.info" fontWeight="bold">
                <RouterLink to={getTaskInstanceLink(original.task_instance)}>
                  <Time datetime={original.task_instance.run_after} />
                </RouterLink>
              </Link>
            ) : (
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
    accessorKey: "response_received",
    header: translate("state.responseReceived"),
  },
  {
    accessorKey: "response_at",
    cell: ({ row: { original } }) => <Time datetime={original.response_at} />,
    header: translate("response.received"),
  },
];

export const HITLTaskInstances = () => {
  const { t: translate } = useTranslation("hitl");
  const { dagId, groupId, runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;
  const responseReceived = searchParams.get(SearchParamsKeys.RESPONSE_RECEIVED);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useHumanInTheLoopServiceGetHitlDetails(
    {
      dagId,
      dagRunId: runId,
      responseReceived: Boolean(responseReceived) ? responseReceived === "true" : undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
      taskIdPattern: groupId,
    },
    undefined,
    {
      enabled: !isNaN(pagination.pageSize),
      refetchInterval,
    },
  );

  return (
    <Box>
      {!Boolean(dagId) && !Boolean(runId) && !Boolean(taskId) ? (
        <Heading size="md">
          {data?.total_entries} {translate("requiredAction", { count: data?.total_entries })}
        </Heading>
      ) : undefined}
      <DataTable
        columns={taskInstanceColumns({
          dagId,
          runId,
          taskId: Boolean(groupId) ? undefined : taskId,
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
    </Box>
  );
};
