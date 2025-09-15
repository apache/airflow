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
import { Box, Heading, Link, createListCollection } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useCallback } from "react";
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
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { getHITLState } from "src/utils/hitl";
import { getTaskInstanceLink } from "src/utils/links";

type TaskInstanceRow = { row: { original: HITLDetail } };

const { OFFSET: OFFSET_PARAM, RESPONSE_RECEIVED: RESPONSE_RECEIVED_PARAM }: SearchParamsKeysType =
  SearchParamsKeys;

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

  const { data, error, isLoading } = useHumanInTheLoopServiceGetHitlDetails({
    dagId,
    dagRunId: runId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy: sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : [],
    responseReceived: Boolean(responseReceived) ? responseReceived === "true" : undefined,
    state: responseReceived === "false" ? ["deferred"] : undefined,
    taskId,
  });

  const enabledOptions = createListCollection({
    items: [
      { label: translate("filters.response.all"), value: "all" },
      { label: translate("filters.response.pending"), value: "false" },
      { label: translate("filters.response.received"), value: "true" },
    ],
  });

  const handleResponseChange = useCallback(
    ({ value }: { value: Array<string> }) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(RESPONSE_RECEIVED_PARAM);
      } else {
        searchParams.set(RESPONSE_RECEIVED_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      searchParams.delete(OFFSET_PARAM);
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams, pagination, sorting, setTableURLState],
  );

  return (
    <Box>
      {!Boolean(dagId) && !Boolean(runId) && !Boolean(taskId) ? (
        <Heading size="md">
          {data?.total_entries} {translate("requiredAction", { count: data?.total_entries })}
        </Heading>
      ) : undefined}
      <Box mt={3}>
        <Select.Root
          collection={enabledOptions}
          maxW="250px"
          onValueChange={handleResponseChange}
          value={[responseReceived ?? "all"]}
        >
          <Select.Label fontSize="xs">{translate("requiredActionState")}</Select.Label>
          <Select.Trigger isActive={Boolean(responseReceived)}>
            <Select.ValueText />
          </Select.Trigger>
          <Select.Content>
            {enabledOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Box>
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
    </Box>
  );
};
