/* eslint-disable max-lines */

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
import { Flex, HStack, Link, type SelectValueChangeDetails, Text, Box } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse, DagRunState, DagRunType } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { LimitedItemsList } from "src/components/LimitedItemsList";
import { MarkRunAsButton } from "src/components/MarkAs";
import RenderedJsonField from "src/components/RenderedJsonField";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { SearchBar } from "src/components/SearchBar";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { dagRunTypeOptions, dagRunStateOptions as stateOptions } from "src/constants/stateOptions";
import DeleteRunButton from "src/pages/DeleteRunButton";
import { renderDuration, useAutoRefresh, isStatePending } from "src/utils";

type DagRunRow = { row: { original: DAGRunResponse } };
const {
  END_DATE: END_DATE_PARAM,
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
  const [searchParams, setSearchParams] = useSearchParams();

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
  const startDate = searchParams.get(START_DATE_PARAM);
  const endDate = searchParams.get(END_DATE_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useDagRunServiceGetDagRuns(
    {
      dagId: dagId ?? "~",
      endDateLte: endDate ?? undefined,
      limit: pageSize,
      offset: pageIndex * pageSize,
      orderBy,
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

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.set(STATE_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleTypeChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(RUN_TYPE_PARAM);
      } else {
        searchParams.set(RUN_TYPE_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleRunIdPatternChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete(RUN_ID_PATTERN_PARAM);
      } else {
        searchParams.set(RUN_ID_PATTERN_PARAM, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleTriggeringUserNamePatternChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete(TRIGGERING_USER_NAME_PATTERN_PARAM);
      } else {
        searchParams.set(TRIGGERING_USER_NAME_PATTERN_PARAM, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );
  const columns = useMemo(() => runColumns(translate, dagId), [translate, dagId]);

  return (
    <>
      <HStack paddingY="4px">
        <Box>
          <SearchBar
            defaultValue={filteredRunIdPattern ?? ""}
            hideAdvanced
            hotkeyDisabled={false}
            onChange={handleRunIdPatternChange}
            placeHolder={translate("dags:filters.runIdPatternFilter")}
          />
        </Box>
        <Box>
          <SearchBar
            defaultValue={filteredTriggeringUserNamePattern ?? ""}
            hideAdvanced
            hotkeyDisabled={true}
            onChange={handleTriggeringUserNamePatternChange}
            placeHolder={translate("dags:filters.triggeringUserNameFilter")}
          />
        </Box>
        <Select.Root
          collection={stateOptions}
          maxW="200px"
          onValueChange={handleStateChange}
          value={[filteredState ?? "all"]}
        >
          <Select.Trigger colorPalette="brand" isActive={Boolean(filteredState)} minW="max-content">
            <Select.ValueText width="auto">
              {() =>
                filteredState === null ? (
                  translate("dags:filters.allStates")
                ) : (
                  <StateBadge state={filteredState as DagRunState}>
                    {translate(`common:states.${filteredState}`)}
                  </StateBadge>
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {stateOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  translate(option.label)
                ) : (
                  <StateBadge state={option.value as DagRunState}>{translate(option.label)}</StateBadge>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>

        <Select.Root
          collection={dagRunTypeOptions}
          maxW="200px"
          onValueChange={handleTypeChange}
          value={[filteredType ?? "all"]}
        >
          <Select.Trigger colorPalette="brand" isActive={Boolean(filteredType)} minW="max-content">
            <Select.ValueText width="auto">
              {() =>
                filteredType === null ? (
                  translate("dags:filters.allRunTypes")
                ) : (
                  <Flex alignItems="center" gap={1}>
                    <RunTypeIcon runType={filteredType as DagRunType} />
                    {translate(`common:runTypes.${filteredType}`)}
                  </Flex>
                )
              }
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content>
            {dagRunTypeOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.value === "all" ? (
                  translate(option.label)
                ) : (
                  <Flex gap={1}>
                    <RunTypeIcon runType={option.value as DagRunType} />
                    {translate(option.label)}
                  </Flex>
                )}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </HStack>
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
