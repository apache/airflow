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
import { Box, Button, Flex, Heading, Text, type ButtonProps } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { MdPause, MdPlayArrow, MdStop } from "react-icons/md";
import { useParams } from "react-router-dom";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceListBackfillsUi,
  useBackfillServiceListBackfillsUiKey,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";
import type { BackfillResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { getDuration, useAutoRefresh } from "src/utils";

const buttonProps = {
  rounded: "full",
  size: "xs",
  variant: "outline",
} satisfies ButtonProps;

type GetColumnsParams = {
  readonly isCancelPending: boolean;
  readonly isPausePending: boolean;
  readonly isUnpausePending: boolean;
  readonly onCancel: (backfill: BackfillResponse) => void;
  readonly onPause: (backfill: BackfillResponse) => void;
  readonly translate: (key: string) => string;
};

const getColumns = ({
  isCancelPending,
  isPausePending,
  isUnpausePending,
  onCancel,
  onPause,
  translate,
}: GetColumnsParams): Array<ColumnDef<BackfillResponse>> => [
  {
    accessorKey: "date_from",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.from_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.from"),
  },
  {
    accessorKey: "date_to",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.to_date} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.to"),
  },
  {
    accessorKey: "reprocess_behavior",
    cell: ({ row }) => (
      <Text>
        {row.original.reprocess_behavior === "none"
          ? translate("components:backfill.missingRuns")
          : row.original.reprocess_behavior === "failed"
            ? translate("components:backfill.missingAndErroredRuns")
            : translate("components:backfill.allRuns")}
      </Text>
    ),
    enableSorting: false,
    header: translate("components:backfill.reprocessBehavior"),
  },
  {
    accessorKey: "created_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.created_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.createdAt"),
  },
  {
    accessorKey: "completed_at",
    cell: ({ row }) => (
      <Text>
        <Time datetime={row.original.completed_at} />
      </Text>
    ),
    enableSorting: false,
    header: translate("table.completedAt"),
  },
  {
    accessorKey: "duration",
    cell: ({ row }) => (
      <Text>
        {row.original.completed_at === null
          ? ""
          : getDuration(row.original.created_at, row.original.completed_at)}
      </Text>
    ),
    enableSorting: false,
    header: translate("duration"),
  },
  {
    accessorKey: "max_active_runs",
    enableSorting: false,
    header: translate("table.maxActiveRuns"),
  },
  {
    accessorKey: "actions",
    cell: ({ row }) => {
      const backfill = row.original;

      if (backfill.completed_at !== null) {
        return null;
      }

      return (
        <Flex gap={2} justifyContent="end">
          <Button
            aria-label={
              backfill.is_paused
                ? translate("components:banner.unpause")
                : translate("components:banner.pause")
            }
            loading={isPausePending || isUnpausePending}
            onClick={() => onPause(backfill)}
            {...buttonProps}
          >
            {backfill.is_paused ? <MdPlayArrow /> : <MdPause />}
          </Button>
          <Button
            aria-label={translate("components:banner.cancel")}
            loading={isCancelPending}
            onClick={() => onCancel(backfill)}
            {...buttonProps}
          >
            <MdStop />
          </Button>
        </Flex>
      );
    },
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const Backfills = () => {
  const { t: translate } = useTranslation();
  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination } = tableURLState;

  const { dagId = "" } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillsUi(
    {
      dagId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.backfills.some((bf: BackfillResponse) => bf.completed_at === null && !bf.is_paused)
          ? refetchInterval
          : false,
    },
  );

  const queryClient = useQueryClient();
  const onSuccess = async () => {
    await queryClient.invalidateQueries({
      queryKey: [useBackfillServiceListBackfillsUiKey],
    });
  };

  const { isPending: isPausePending, mutate: pauseMutate } = useBackfillServicePauseBackfill({ onSuccess });
  const { isPending: isUnpausePending, mutate: unpauseMutate } = useBackfillServiceUnpauseBackfill({
    onSuccess,
  });
  const { isPending: isCancelPending, mutate: cancelMutate } = useBackfillServiceCancelBackfill({
    onSuccess,
  });

  const handlePause = useCallback(
    (backfill: BackfillResponse) => {
      if (backfill.is_paused) {
        unpauseMutate({ backfillId: backfill.id });
      } else {
        pauseMutate({ backfillId: backfill.id });
      }
    },
    [pauseMutate, unpauseMutate],
  );

  const handleCancel = useCallback(
    (backfill: BackfillResponse) => {
      cancelMutate({ backfillId: backfill.id });
    },
    [cancelMutate],
  );

  const columns = getColumns({
    isCancelPending,
    isPausePending,
    isUnpausePending,
    onCancel: handleCancel,
    onPause: handlePause,
    translate,
  });

  return (
    <Box>
      <ErrorAlert error={error} />
      <Heading my={1} size="md">
        {translate("backfill", { count: data ? data.total_entries : 0 })}
      </Heading>
      <DataTable
        columns={columns}
        data={data ? data.backfills : []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:backfill"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
