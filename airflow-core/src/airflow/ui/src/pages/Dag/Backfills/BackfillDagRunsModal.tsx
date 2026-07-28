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
import { Heading, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useBackfillServiceListBackfillDagRuns } from "openapi/queries";
import type { BackfillDagRunResponse, BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import type { TableState } from "src/components/DataTable/types";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Dialog, RouterLink } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";
import { useAutoRefresh } from "src/utils";

type BackfillDagRunsModalProps = {
  readonly backfill: BackfillResponse | undefined;
  readonly onClose: () => void;
  readonly open: boolean;
};

const translateExceptionReason = (reason: string, translate: (key: string) => string) => {
  // Keep this mapping in sync with BackfillDagRunExceptionReason in airflow.models.backfill.
  switch (reason) {
    case "already exists":
      return translate("components:backfill.exceptionReason.alreadyExists");
    case "in flight":
      return translate("components:backfill.exceptionReason.inFlight");
    default:
      return translate("components:backfill.exceptionReason.unknown");
  }
};

const getColumns = (translate: (key: string) => string): Array<ColumnDef<BackfillDagRunResponse>> => [
  {
    accessorKey: "logical_date",
    cell: ({ row }) => {
      if (row.original.partition_key !== null && row.original.partition_key !== "") {
        return <Text>{row.original.partition_key}</Text>;
      }

      if (row.original.logical_date !== null && row.original.logical_date !== "") {
        return (
          <Text>
            <Time datetime={row.original.logical_date} />
          </Text>
        );
      }

      return <Text color="fg.muted">—</Text>;
    },
    enableSorting: false,
    header: translate("slot"),
  },
  {
    accessorKey: "dag_run_state",
    cell: ({ row }) => {
      const state = row.original.dag_run_state;

      if (state === null || state === undefined) {
        return <Text color="fg.muted">—</Text>;
      }

      return <StateBadge state={state}>{translate(`states.${state}`)}</StateBadge>;
    },
    enableSorting: false,
    header: translate("dagRunState"),
  },
  {
    accessorKey: "exception_reason",
    cell: ({ row }) => {
      const reason = row.original.exception_reason;

      if (reason === null || reason === "") {
        return <Text color="fg.muted">—</Text>;
      }

      return <Text>{translateExceptionReason(reason, translate)}</Text>;
    },
    enableSorting: false,
    header: translate("components:backfill.notCreatedReason"),
  },
  {
    accessorKey: "dag_run_id",
    cell: ({ row }) => {
      const runId = row.original.dag_run_id;

      if (runId === null || runId === undefined || runId === "") {
        return <Text color="fg.muted">—</Text>;
      }

      return (
        <RouterLink fontWeight="bold" to={`/dags/${row.original.dag_id}/runs/${runId}`}>
          {runId}
        </RouterLink>
      );
    },
    enableSorting: false,
    header: translate("runId"),
  },
  {
    accessorKey: "sort_ordinal",
    enableSorting: false,
    header: "#",
  },
];

export const BackfillDagRunsModal = ({ backfill, onClose, open }: BackfillDagRunsModalProps) => {
  const { t: translate } = useTranslation();
  const pageSize = (useConfig("fallback_page_limit") as number | undefined) ?? 100;
  const [pageIndex, setPageIndex] = useState(0);
  const tableState = {
    pagination: {
      pageIndex,
      pageSize,
    },
    sorting: [],
  } satisfies TableState;
  const refetchInterval = useAutoRefresh({ dagId: backfill?.dag_id });
  const shouldPoll = backfill?.completed_at === null && !backfill.is_paused;

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillDagRuns(
    {
      backfillId: backfill?.id ?? 0,
      limit: pageSize,
      offset: pageIndex * pageSize,
    },
    undefined,
    {
      enabled: open && backfill !== undefined,
      refetchInterval: shouldPoll ? refetchInterval : false,
    },
  );

  const handleOpenChange = () => {
    setPageIndex(0);
    onClose();
  };

  return (
    <Dialog.Root
      lazyMount
      onOpenChange={handleOpenChange}
      open={open}
      scrollBehavior="inside"
      size="cover"
      unmountOnExit
    >
      <Dialog.Content backdrop>
        <Dialog.Header>
          <Heading size="md">
            {translate("common:backfill_one")} #{backfill?.id}
          </Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body>
          <ErrorAlert error={error} />
          <DataTable
            columns={getColumns(translate)}
            data={data?.backfill_dag_runs ?? []}
            initialState={tableState}
            isFetching={isFetching}
            isLoading={isLoading}
            modelName="common:slot"
            onStateChange={(state) => setPageIndex(state.pagination.pageIndex)}
            showRowCountHeading
            total={data?.total_entries ?? 0}
          />
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
