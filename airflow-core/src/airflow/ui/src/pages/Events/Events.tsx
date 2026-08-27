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
import { Code, useDisclosure } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useEventLogServiceGetEventLogs } from "openapi/queries";
import type { EventLogResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ExpandCollapseButtons } from "src/components/ExpandCollapseButtons";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useAdvancedSearchArg } from "src/hooks/useAdvancedSearch";
import { useConfig } from "src/queries/useConfig";
import { useDocumentTitle } from "src/utils";

import { EventsFilters } from "./EventsFilters";

type EventsColumn = {
  dagId?: string;
  multiTeam: boolean;
  open?: boolean;
  runId?: string;
  taskId?: string;
};

const eventsColumn = (
  { dagId, multiTeam, open, runId, taskId }: EventsColumn,
  translate: (key: string) => string,
): Array<ColumnDef<EventLogResponse>> => [
  {
    accessorKey: "when",
    cell: ({ row: { original } }) => <Time datetime={original.when} />,
    enableSorting: true,
    header: translate("auditLog.columns.when"),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "event",
    enableSorting: true,
    header: translate("auditLog.columns.event"),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "owner",
    cell: ({ row: { original } }) => original.owner_display_name,
    enableSorting: true,
    header: translate("auditLog.columns.user"),
    meta: {
      skeletonWidth: 10,
    },
  },
  ...(multiTeam
    ? [
        {
          accessorKey: "team_name",
          enableSorting: false,
          header: translate("common:dagDetails.team"),
          meta: {
            skeletonWidth: 10,
          },
        },
      ]
    : []),
  {
    accessorKey: "extra",
    cell: ({ row: { original } }) => {
      if (original.extra !== null) {
        try {
          const parsed = JSON.parse(original.extra) as Record<string, unknown>;

          return <RenderedJsonField collapsed={!open} content={parsed} />;
        } catch {
          return <Code>{original.extra}</Code>;
        }
      }

      return undefined;
    },
    enableSorting: false,
    header: translate("auditLog.columns.extra"),
    meta: {
      skeletonWidth: 200,
    },
  },
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_id",
          enableSorting: true,
          header: translate("common:dagId"),
          meta: {
            skeletonWidth: 10,
          },
        },
      ]),
  ...(Boolean(runId)
    ? []
    : [
        {
          accessorKey: "run_id",
          enableSorting: true,
          header: translate("common:runId"),
          meta: {
            skeletonWidth: 10,
          },
        },
      ]),
  ...(Boolean(taskId)
    ? []
    : [
        {
          accessorKey: "task_id",
          enableSorting: true,
          header: translate("common:taskId"),
          meta: {
            skeletonWidth: 10,
          },
        },
      ]),
  {
    accessorKey: "map_index",
    enableSorting: false,
    header: translate("common:mapIndex"),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "try_number",
    enableSorting: false,
    header: translate("common:tryNumber"),
    meta: {
      skeletonWidth: 10,
    },
  },
];

const {
  AFTER: AFTER_PARAM,
  BEFORE: BEFORE_PARAM,
  DAG_ID: DAG_ID_PARAM,
  EVENT_TYPE: EVENT_TYPE_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  RUN_ID: RUN_ID_PARAM,
  TASK_ID: TASK_ID_PARAM,
  TEAMS: TEAMS_PARAM,
  TRY_NUMBER: TRY_NUMBER_PARAM,
  USER: USER_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

export const Events = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { dagId, runId, taskId } = useParams();
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  // Only the standalone audit-log page owns the tab title; nested tabs inherit their parent page's title.
  useDocumentTitle(dagId === undefined ? translate("common:browse.auditLog") : undefined);

  const [searchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const { onClose, onOpen, open } = useDisclosure();

  const afterFilter = searchParams.get(AFTER_PARAM);
  const beforeFilter = searchParams.get(BEFORE_PARAM);
  const dagIdFilter = searchParams.get(DAG_ID_PARAM);
  const eventTypeFilter = searchParams.get(EVENT_TYPE_PARAM);
  const mapIndexFilter = searchParams.get(MAP_INDEX_PARAM);
  const runIdFilter = searchParams.get(RUN_ID_PARAM);
  const taskIdFilter = searchParams.get(TASK_ID_PARAM);
  const tryNumberFilter = searchParams.get(TRY_NUMBER_PARAM);
  const userFilter = searchParams.get(USER_PARAM);
  const teams = searchParams.getAll(TEAMS_PARAM);

  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-when"];
  // Convert string filters to appropriate types for API
  const mapIndexNumber = mapIndexFilter === null ? undefined : parseInt(mapIndexFilter, 10);
  const tryNumberNumber = tryNumberFilter === null ? undefined : parseInt(tryNumberFilter, 10);
  // Handle date conversion - ensure valid ISO strings
  const afterDate = afterFilter !== null && dayjs(afterFilter).isValid() ? afterFilter : undefined;
  const beforeDate = beforeFilter !== null && dayjs(beforeFilter).isValid() ? beforeFilter : undefined;

  const dagIdArg = useAdvancedSearchArg({
    patternApiKey: "dagIdPattern",
    prefixApiKey: "dagIdPrefixPattern",
    storageKey: DAG_ID_PARAM,
    value: dagIdFilter,
  });
  const eventArg = useAdvancedSearchArg({
    patternApiKey: "eventPattern",
    prefixApiKey: "eventPrefixPattern",
    storageKey: EVENT_TYPE_PARAM,
    value: eventTypeFilter,
  });
  const ownerArg = useAdvancedSearchArg({
    patternApiKey: "ownerDisplayNamePattern",
    prefixApiKey: "ownerDisplayNamePrefixPattern",
    storageKey: USER_PARAM,
    value: userFilter,
  });
  const runIdArg = useAdvancedSearchArg({
    patternApiKey: "runIdPattern",
    prefixApiKey: "runIdPrefixPattern",
    storageKey: RUN_ID_PARAM,
    value: runIdFilter,
  });
  const taskIdArg = useAdvancedSearchArg({
    patternApiKey: "taskIdPattern",
    prefixApiKey: "taskIdPrefixPattern",
    storageKey: TASK_ID_PARAM,
    value: taskIdFilter,
  });

  const { data, error, isFetching, isLoading } = useEventLogServiceGetEventLogs(
    {
      after: afterDate,
      before: beforeDate,
      // Use exact match for URL params (dag/run/task context)
      dagId: dagId ?? undefined,
      // Use pattern search for filter inputs (partial matching)
      ...dagIdArg,
      ...eventArg,
      limit: pagination.pageSize,
      mapIndex: mapIndexNumber,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      ...ownerArg,
      runId: runId ?? undefined,
      ...runIdArg,
      taskId: taskId ?? undefined,
      ...taskIdArg,
      teams: teams.length > 0 ? teams : undefined,
      tryNumber: tryNumberNumber,
    },
    undefined,
  );

  const eventLogs = data?.event_logs ?? [];
  const columns = eventsColumn({ dagId, multiTeam: multiTeamEnabled, open, runId, taskId }, translate);

  return (
    <>
      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={eventLogs}
        displayMode="table"
        filterActions={<EventsFilters urlDagId={dagId} urlRunId={runId} urlTaskId={taskId} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:event"
        onStateChange={setTableURLState}
        presentationActions={
          eventLogs.length > 0 ? (
            <ExpandCollapseButtons
              collapseLabel={translate("common:collapseAllExtra")}
              expandLabel={translate("common:expandAllExtra")}
              isExpanded={open}
              onCollapse={onClose}
              onExpand={onOpen}
            />
          ) : undefined
        }
        skeletonCount={undefined}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
