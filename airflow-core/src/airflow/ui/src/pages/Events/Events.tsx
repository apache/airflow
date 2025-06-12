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
import { Box, ButtonGroup, Code, Flex, Heading, IconButton, useDisclosure } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { MdCompress, MdExpand } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useEventLogServiceGetEventLogs } from "openapi/queries";
import type { EventLogResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";

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

export const Events = () => {
  const { t: translate } = useTranslation("browse");
  const { dagId, runId, taskId } = useParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const { onClose, onOpen, open } = useDisclosure();

  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-when";

  const { data, error, isFetching, isLoading } = useEventLogServiceGetEventLogs(
    {
      dagId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      runId,
      taskId,
    },
    undefined,
    { enabled: !isNaN(pagination.pageSize) },
  );

  return (
    <Box>
      <Flex alignItems="center" justifyContent="space-between">
        {dagId === undefined && runId === undefined && taskId === undefined ? (
          <Heading size="md">{translate("auditLog.title")}</Heading>
        ) : undefined}
        <ButtonGroup attached mt="1" size="sm" variant="surface">
          <IconButton
            aria-label={translate("auditLog.actions.expandAllExtra")}
            onClick={onOpen}
            size="sm"
            title={translate("auditLog.actions.expandAllExtra")}
            variant="surface"
          >
            <MdExpand />
          </IconButton>
          <IconButton
            aria-label={translate("auditLog.actions.collapseAllExtra")}
            onClick={onClose}
            size="sm"
            title={translate("auditLog.actions.collapseAllExtra")}
            variant="surface"
          >
            <MdCompress />
          </IconButton>
        </ButtonGroup>
      </Flex>
      <ErrorAlert error={error} />
      <DataTable
        columns={eventsColumn({ dagId, open, runId, taskId }, translate)}
        data={data ? data.event_logs : []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName={translate("auditLog.columns.event")}
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
