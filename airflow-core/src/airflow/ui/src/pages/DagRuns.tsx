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
import { Flex, HStack, Link, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import RenderedJsonField from "src/components/RenderedJsonField";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { DagRunsFilters } from "src/pages/DagRunsFilters";
import DeleteRunButton from "src/pages/DeleteRunButton";
import { renderDuration, useAutoRefresh, isStatePending } from "src/utils";

type DagRunRow = { row: { original: DAGRunResponse } };
const {
  CONF_CONTAINS: CONF_CONTAINS_PARAM,
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  DAG_VERSION: DAG_VERSION_PARAM,
  DURATION_GTE: DURATION_GTE_PARAM,
  DURATION_LTE: DURATION_LTE_PARAM,
  END_DATE: END_DATE_PARAM,
  RUN_AFTER_GTE: RUN_AFTER_GTE_PARAM,
  RUN_AFTER_LTE: RUN_AFTER_LTE_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  RUN_TYPE: RUN_TYPE_PARAM,
  START_DATE: START_DATE_PARAM,
  STATE: STATE_PARAM,
  TRIGGERING_USER_NAME_PATTERN: TRIGGERING_USER_NAME_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const runColumns = (translate: TFunction, dagId?: string): Array<ColumnDef<DAGRunResponse>> => [
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_display_name",
          cell: ({ row: { original } }: DagRunRow) => (
            <Link asChild color="fg.info">
              <RouterLink to={`/dags/${original.dag_id}`}>
                <TruncatedText text={original.dag_display_name} />
              </RouterLink>
            </Link>
          ),
          enableSorting: false,
          header: translate("dagId"),
        },
      ]),
  {
    accessorKey: "dag_run_id",
    cell: ({ row: { original } }: DagRunRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.dag_run_id}`}>
          <TruncatedText text={original.dag_run_id} />
        </RouterLink>
      </Link>
    ),
    header: translate("dagRunId"),
  },
  {
    accessorKey: "run_after",
    cell: ({ row: { original } }: DagRunRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dags/${original.dag_id}/runs/${original.dag_run_id}`}>
          <Time datetime={original.run_after} />
        </RouterLink>
      </Link>
    ),
    header: translate("dagRun.runAfter"),
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => <StateBadge state={state}>{translate(`common:states.${state}`)}</StateBadge>,
    header: () => translate("state"),
  },
  {
    accessorKey: "run_type",
    cell: ({ row: { original } }) => (
      <HStack>
        <RunTypeIcon runType={original.run_type} />
        <Text>{translate(`common:runTypes.${original.run_type}`)}</Text>
      </HStack>
    ),
    enableSorting: false,
    header: translate("dagRun.runType"),
  },
  {
    accessorKey: "triggering_user_name",
    cell: ({ row: { original } }) => <Text>{original.triggering_user_name ?? ""}</Text>,
    enableSorting: false,
    header: translate("dagRun.triggeringUser"),
  },
  {
    accessorKey: "start_date",
    cell: ({ row: { original } }) => <Time datetime={original.start_date} />,
    header: translate("startDate"),
  },
  {
    accessorKey: "end_date",
    cell: ({ row: { original } }) => <Time datetime={original.end_date} />,
    header: translate("endDate"),
  },
  {
    accessorKey: "duration",
    cell: ({ row: { original } }) => renderDuration(original.duration),
    header: translate("duration"),
  },
  {
    accessorKey: "dag_versions",
    cell: ({ row: { original } }) => (
      <LimitedItemsList
        items={original.dag_versions.map((version) => (
          <DagVersion key={version.id} version={version} />
        ))}
        maxItems={4}
      />
    ),
    enableSorting: false,
    header: translate("dagRun.dagVersions"),
  },
  {
    accessorKey: "conf",
    cell: ({ row: { original } }) =>
      original.conf && Object.keys(original.conf).length > 0 ? (
        <RenderedJsonField content={original.conf} jsonProps={{ collapsed: true }} />
      ) : undefined,
    header: translate("dagRun.conf"),
  },
  {
    accessorKey: "actions",
    cell: ({ row }) => (
      <Flex justifyContent="end">
        <ClearRunButton dagRun={row.original} withText={false} />
        <MarkRunAsButton dagRun={row.original} withText={false} />
        <DeleteRunButton dagRun={row.original} withText={false} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const DagRuns = () => {
  const { t: translate } = useTranslation();
  const { dagId } = useParams();
  const [searchParams] = useSearchParams();

  const { setTableURLState, tableURLState } = useTableURLState({
    columnVisibility: {
      conf: false,
      dag_version: false,
      end_date: false,
    },
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-run_after"];

  const { pageIndex, pageSize } = pagination;
  const filteredState = searchParams.get(STATE_PARAM);
  const filteredType = searchParams.get(RUN_TYPE_PARAM);
  const filteredRunIdPattern = searchParams.get(RUN_ID_PATTERN_PARAM);
  const filteredTriggeringUserNamePattern = searchParams.get(TRIGGERING_USER_NAME_PATTERN_PARAM);
  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const filteredDagVersion = searchParams.get(DAG_VERSION_PARAM);
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);
  const runAfterGte = searchParams.get(RUN_AFTER_GTE_PARAM);
  const runAfterLte = searchParams.get(RUN_AFTER_LTE_PARAM);
  const durationGte = searchParams.get(DURATION_GTE_PARAM);
  const durationLte = searchParams.get(DURATION_LTE_PARAM);
  const confContains = searchParams.get(CONF_CONTAINS_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useDagRunServiceGetDagRuns(
    {
      confContains: confContains !== null && confContains !== "" ? confContains : undefined,
      dagId: dagId ?? "~",
      dagIdPattern: filteredDagIdPattern ?? undefined,
      dagVersion:
        filteredDagVersion !== null && filteredDagVersion !== "" ? [Number(filteredDagVersion)] : undefined,
      durationGte: durationGte !== null && durationGte !== "" ? Number(durationGte) : undefined,
      durationLte: durationLte !== null && durationLte !== "" ? Number(durationLte) : undefined,
      endDateLte: endDate ?? undefined,
      limit: pageSize,
      offset: pageIndex * pageSize,
      orderBy,
      runAfterGte: runAfterGte ?? undefined,
      runAfterLte: runAfterLte ?? undefined,
      runIdPattern: filteredRunIdPattern ?? undefined,
      runType: filteredType === null ? undefined : [filteredType],
      startDateGte: startDate ?? undefined,
      state: filteredState === null ? undefined : [filteredState],
      triggeringUserNamePattern: filteredTriggeringUserNamePattern ?? undefined,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((run) => isStatePending(run.state)) ? refetchInterval : false,
    },
  );

  const columns = useMemo(() => runColumns(translate, dagId), [translate, dagId]);

  return (
    <>
      <DagRunsFilters dagId={dagId} />
      <DataTable
        columns={columns}
        data={data?.dag_runs ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName={translate("common:dagRun_other")}
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </>
  );
};
