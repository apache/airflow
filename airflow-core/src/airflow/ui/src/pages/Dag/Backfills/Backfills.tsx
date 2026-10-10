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
import { Badge, Button, HStack, Text } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { MdPause, MdPlayArrow, MdStop } from "react-icons/md";
import { useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";

import {
  useBackfillServiceCancelBackfill,
  useBackfillServiceListBackfillsUi,
  useBackfillServiceListBackfillsUiKey,
  useBackfillServicePauseBackfill,
  useBackfillServiceUnpauseBackfill,
} from "openapi/queries";
import type { BackfillResponse, ReprocessBehavior } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { type DurationFormat, useDurationFormat } from "src/utils";

import { BackfillDagRunsModal } from "./BackfillDagRunsModal";
import { BackfillsFilters } from "./BackfillsFilters";

const {
  COMPLETED_AT_GTE: COMPLETED_AT_GTE_PARAM,
  COMPLETED_AT_LTE: COMPLETED_AT_LTE_PARAM,
  CREATED_AT_GTE: CREATED_AT_GTE_PARAM,
  CREATED_AT_LTE: CREATED_AT_LTE_PARAM,
  DURATION_GTE: DURATION_GTE_PARAM,
  DURATION_LTE: DURATION_LTE_PARAM,
  FROM_DATE_GTE: FROM_DATE_GTE_PARAM,
  FROM_DATE_LTE: FROM_DATE_LTE_PARAM,
  MAX_ACTIVE_RUNS_GTE: MAX_ACTIVE_RUNS_GTE_PARAM,
  MAX_ACTIVE_RUNS_LTE: MAX_ACTIVE_RUNS_LTE_PARAM,
  REPROCESS_BEHAVIOR: REPROCESS_BEHAVIOR_PARAM,
  TO_DATE_GTE: TO_DATE_GTE_PARAM,
  TO_DATE_LTE: TO_DATE_LTE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const REPROCESS_BEHAVIOR_VALUES = [
  "failed",
  "completed",
  "none",
] as const satisfies ReadonlyArray<ReprocessBehavior>;

const isReprocessBehavior = (value: string | null): value is ReprocessBehavior =>
  (REPROCESS_BEHAVIOR_VALUES as ReadonlyArray<string | null>).includes(value);

type ColumnProps = {
  readonly isActionPending: boolean;
  readonly onCancel: (backfillId: number) => void;
  readonly onSelectBackfill: (backfillId: number) => void;
  readonly onTogglePause: (backfill: BackfillResponse) => void;
  readonly translate: TFunction;
} & Pick<DurationFormat, "formatElapsed">;

const getColumns = ({
  formatElapsed,
  isActionPending,
  onCancel,
  onSelectBackfill,
  onTogglePause,
  translate,
}: ColumnProps): Array<ColumnDef<DataTableFeatures, BackfillResponse>> => [
  {
    accessorKey: "date_from",
    cell: ({ row }) => (
      <Button
        aria-label={translate("components:backfill.viewSlots", { id: row.original.id })}
        colorPalette="brand"
        fontWeight="bold"
        onClick={() => onSelectBackfill(row.original.id)}
        variant="plain"
      >
        <Time datetime={row.original.from_date} />
      </Button>
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
    cell: ({ row }) =>
      row.original.completed_at === null ? (
        <Badge colorPalette="info" variant="subtle">
          {row.original.is_paused
            ? translate("dags:schedulingState.paused")
            : translate("components:banner.backfillInProgress")}
        </Badge>
      ) : (
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
          : formatElapsed(row.original.created_at, row.original.completed_at)}
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
    cell: ({ row }) =>
      row.original.completed_at === null ? (
        <HStack gap={1}>
          <IconButton
            colorPalette="info"
            label={
              row.original.is_paused
                ? translate("components:banner.unpause")
                : translate("components:banner.pause")
            }
            loading={isActionPending}
            onClick={() => onTogglePause(row.original)}
            size="xs"
            variant="outline"
          >
            {row.original.is_paused ? <MdPlayArrow /> : <MdPause />}
          </IconButton>
          <IconButton
            colorPalette="info"
            label={translate("components:banner.cancel")}
            loading={isActionPending}
            onClick={() => onCancel(row.original.id)}
            size="xs"
            variant="outline"
          >
            <MdStop />
          </IconButton>
        </HStack>
      ) : undefined,
    enableSorting: false,
    header: "",
  },
];

export const Backfills = () => {
  const { t: translate } = useTranslation(["common", "components", "dags"]);
  const { formatElapsed } = useDurationFormat();
  const { setTableURLState, tableURLState } = useTableURLState();
  const location = useLocation();
  const navigate = useNavigate();

  const { pagination } = tableURLState;

  const { backfillId, dagId = "" } = useParams();
  const selectedBackfillId = Number(backfillId);
  const hasSelectedBackfill = Number.isInteger(selectedBackfillId) && selectedBackfillId > 0;

  const [searchParams] = useSearchParams();

  const fromDateGte = searchParams.get(FROM_DATE_GTE_PARAM);
  const fromDateLte = searchParams.get(FROM_DATE_LTE_PARAM);
  const toDateGte = searchParams.get(TO_DATE_GTE_PARAM);
  const toDateLte = searchParams.get(TO_DATE_LTE_PARAM);
  const createdAtGte = searchParams.get(CREATED_AT_GTE_PARAM);
  const createdAtLte = searchParams.get(CREATED_AT_LTE_PARAM);
  const completedAtGte = searchParams.get(COMPLETED_AT_GTE_PARAM);
  const completedAtLte = searchParams.get(COMPLETED_AT_LTE_PARAM);
  const maxActiveRunsGte = searchParams.get(MAX_ACTIVE_RUNS_GTE_PARAM);
  const maxActiveRunsLte = searchParams.get(MAX_ACTIVE_RUNS_LTE_PARAM);
  const durationGte = searchParams.get(DURATION_GTE_PARAM);
  const durationLte = searchParams.get(DURATION_LTE_PARAM);
  const reprocessBehaviorParam = searchParams.get(REPROCESS_BEHAVIOR_PARAM);
  const reprocessBehavior = isReprocessBehavior(reprocessBehaviorParam) ? reprocessBehaviorParam : undefined;

  const { data, error, isFetching, isLoading } = useBackfillServiceListBackfillsUi({
    completedAtGte: completedAtGte ?? undefined,
    completedAtLte: completedAtLte ?? undefined,
    createdAtGte: createdAtGte ?? undefined,
    createdAtLte: createdAtLte ?? undefined,
    dagId,
    durationGte: durationGte !== null && durationGte !== "" ? Number(durationGte) : undefined,
    durationLte: durationLte !== null && durationLte !== "" ? Number(durationLte) : undefined,
    fromDateGte: fromDateGte ?? undefined,
    fromDateLte: fromDateLte ?? undefined,
    limit: pagination.pageSize,
    maxActiveRunsGte:
      maxActiveRunsGte !== null && maxActiveRunsGte !== "" ? Number(maxActiveRunsGte) : undefined,
    maxActiveRunsLte:
      maxActiveRunsLte !== null && maxActiveRunsLte !== "" ? Number(maxActiveRunsLte) : undefined,
    offset: pagination.pageIndex * pagination.pageSize,
    reprocessBehavior,
    toDateGte: toDateGte ?? undefined,
    toDateLte: toDateLte ?? undefined,
  });

  const onSelectBackfill = (id: number) => {
    void Promise.resolve(
      navigate({
        pathname: `/dags/${dagId}/backfills/${id}`,
        search: location.search,
      }),
    );
  };
  const onClose = () => {
    void Promise.resolve(
      navigate(
        {
          pathname: `/dags/${dagId}/backfills`,
          search: location.search,
        },
        { replace: true },
      ),
    );
  };
  const queryClient = useQueryClient();
  const onSuccess = async () => {
    await queryClient.invalidateQueries({ queryKey: [useBackfillServiceListBackfillsUiKey] });
  };
  const { isPending: isPausePending, mutate: pauseMutate } = useBackfillServicePauseBackfill({ onSuccess });
  const { isPending: isUnpausePending, mutate: unpauseMutate } = useBackfillServiceUnpauseBackfill({
    onSuccess,
  });
  const { isPending: isCancelPending, mutate: cancelMutate } = useBackfillServiceCancelBackfill({
    onSuccess,
  });

  const columns = getColumns({
    formatElapsed,
    isActionPending: isPausePending || isUnpausePending || isCancelPending,
    onCancel: (id) => cancelMutate({ backfillId: id }),
    onSelectBackfill,
    onTogglePause: (backfill) =>
      backfill.is_paused
        ? unpauseMutate({ backfillId: backfill.id })
        : pauseMutate({ backfillId: backfill.id }),
    translate,
  });

  return (
    <>
      <BackfillsFilters />
      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data ? data.backfills : []}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:backfill"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
      <BackfillDagRunsModal
        backfillId={hasSelectedBackfill ? selectedBackfillId : undefined}
        dagId={dagId}
        onClose={onClose}
        open={hasSelectedBackfill}
      />
    </>
  );
};
