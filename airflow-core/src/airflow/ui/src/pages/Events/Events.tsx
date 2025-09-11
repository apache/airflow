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
import { ButtonGroup, Code, Flex, Heading, IconButton, useDisclosure, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { MdCompress, MdExpand } from "react-icons/md";
import { useParams, useSearchParams } from "react-router-dom";

import { useEventLogServiceGetEventLogs } from "openapi/queries";
import type { EventLogResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

import { EventsFilters } from "./EventsFilters";

type EventsColumn = {
  dagId?: string;
  open?: boolean;
  runId?: string;
  taskId?: string;
};

const eventsColumn = (
  { dagId, open, runId, taskId }: EventsColumn,
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
    enableSorting: true,
    header: translate("auditLog.columns.user"),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "extra",
    cell: ({ row: { original } }) => {
      if (original.extra !== null) {
        try {
          const parsed = JSON.parse(original.extra) as Record<string, unknown>;

          return <RenderedJsonField content={parsed} jsonProps={{ collapsed: !open }} />;
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
  TRY_NUMBER: TRY_NUMBER_PARAM,
  USER: USER_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

export const Events = () => {
  const { t: translate } = useTranslation("browse");
  const { dagId, runId, taskId } = useParams();
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

  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-when"];
  // Convert string filters to appropriate types for API
  const mapIndexNumber = mapIndexFilter === null ? undefined : parseInt(mapIndexFilter, 10);
  const tryNumberNumber = tryNumberFilter === null ? undefined : parseInt(tryNumberFilter, 10);
  // Handle date conversion - ensure valid ISO strings
  const afterDate = afterFilter !== null && dayjs(afterFilter).isValid() ? afterFilter : undefined;
  const beforeDate = beforeFilter !== null && dayjs(beforeFilter).isValid() ? beforeFilter : undefined;

  const { data, error, isFetching, isLoading } = useEventLogServiceGetEventLogs(
    {
      after: afterDate,
      before: beforeDate,
      // Use exact match for URL params (dag/run/task context)
      dagId: dagId ?? undefined,
      // Use pattern search for filter inputs (partial matching)
      dagIdPattern: dagIdFilter ?? undefined,
      eventPattern: eventTypeFilter ?? undefined,
      limit: pagination.pageSize,
      mapIndex: mapIndexNumber,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      ownerPattern: userFilter ?? undefined,
      runId: runId ?? undefined,
      runIdPattern: runIdFilter ?? undefined,
      taskId: taskId ?? undefined,
      taskIdPattern: taskIdFilter ?? undefined,
      tryNumber: tryNumberNumber,
    },
    undefined,
  );

  return (
    <VStack alignItems="stretch">
      {dagId === undefined && runId === undefined && taskId === undefined ? (
        <Heading size="md">{translate("auditLog.title")}</Heading>
      ) : undefined}
      <Flex alignItems="center" justifyContent="space-between">
        <EventsFilters urlDagId={dagId} urlRunId={runId} urlTaskId={taskId} />
        <ButtonGroup attached mt="1" size="sm" variant="surface">
          <IconButton
            aria-label={translate("auditLog.actions.expandAllExtra")}
            onClick={onOpen}
            size="sm"
            title={translate("auditLog.actions.expandAllExtra")}
          >
            <MdExpand />
          </IconButton>
          <IconButton
            aria-label={translate("auditLog.actions.collapseAllExtra")}
            onClick={onClose}
            size="sm"
            title={translate("auditLog.actions.collapseAllExtra")}
          >
            <MdCompress />
          </IconButton>
        </ButtonGroup>
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={eventsColumn({ dagId, open, runId, taskId }, translate)}
        data={data?.event_logs ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("auditLog.columns.event")}
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data?.total_entries ?? 0}
      />
    </VStack>
  );
};
