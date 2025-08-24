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
import { Flex, HStack, Link, type SelectValueChangeDetails, Text, Box, Button } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse, DagRunState, DagRunType } from "openapi/requests/types.gen";
import { ClearRunButton } from "src/components/Clear";
import { DagVersion } from "src/components/DagVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
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
  DAG_ID: DAG_ID_PARAM,
  END_DATE_GTE: END_DATE_GTE_PARAM,
  END_DATE_LTE: END_DATE_LTE_PARAM,
  RUN_AFTER_GTE: RUN_AFTER_GTE_PARAM,
  RUN_AFTER_LTE: RUN_AFTER_LTE_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  RUN_TYPE: RUN_TYPE_PARAM,
  START_DATE_GTE: START_DATE_GTE_PARAM,
  START_DATE_LTE: START_DATE_LTE_PARAM,
  STATE: STATE_PARAM,
  TRIGGERING_USER_NAME_PATTERN: TRIGGERING_USER_NAME_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const runColumns = (translate: TFunction, dagId?: string): Array<ColumnDef<DAGRunResponse>> => [
  ...(Boolean(dagId)
    ? []
    : [
        {
          accessorKey: "dag_display_name",
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
    }) => <StateBadge state={state}>{state}</StateBadge>,
    header: () => translate("state"),
  },
  {
    accessorKey: "run_type",
    cell: ({ row: { original } }) => (
      <HStack>
        <RunTypeIcon runType={original.run_type} />
        <Text>{original.run_type}</Text>
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

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-run_after"];

  const { pageIndex, pageSize } = pagination;
  const filteredState = searchParams.get(STATE_PARAM);
  const filteredType = searchParams.get(RUN_TYPE_PARAM);
  const filteredRunIdPattern = searchParams.get(RUN_ID_PATTERN_PARAM);
  const filteredDagId = searchParams.get(DAG_ID_PARAM);
  const filteredTriggeringUserNamePattern = searchParams.get(TRIGGERING_USER_NAME_PATTERN_PARAM);
  const startDateGte = searchParams.get(START_DATE_GTE_PARAM);
  const startDateLte = searchParams.get(START_DATE_LTE_PARAM);
  const endDateGte = searchParams.get(END_DATE_GTE_PARAM);
  const endDateLte = searchParams.get(END_DATE_LTE_PARAM);
  const runAfterGte = searchParams.get(RUN_AFTER_GTE_PARAM);
  const runAfterLte = searchParams.get(RUN_AFTER_LTE_PARAM);

  const refetchInterval = useAutoRefresh({});

  const { data, error, isLoading } = useDagRunServiceGetDagRuns(
    {
      dagId: filteredDagId ?? "~",
      endDateGte: endDateGte ?? undefined,
      endDateLte: endDateLte ?? undefined,
      limit: pageSize,
      offset: pageIndex * pageSize,
      orderBy,
      runAfterGte: runAfterGte ?? undefined,
      runAfterLte: runAfterLte ?? undefined,
      runIdPattern: filteredRunIdPattern ?? undefined,
      runType: filteredType === null ? undefined : [filteredType],
      startDateGte: startDateGte ?? undefined,
      startDateLte: startDateLte ?? undefined,
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

  const handleDagIdChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete(DAG_ID_PARAM);
      } else {
        searchParams.set(DAG_ID_PARAM, value);
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

  const handleDateRangeChange = useCallback(
    (gteParam: string, lteParam: string) => (gteValue: string, lteValue: string) => {
      if (gteValue === "") {
        searchParams.delete(gteParam);
      } else {
        searchParams.set(gteParam, gteValue);
      }

      if (lteValue === "") {
        searchParams.delete(lteParam);
      } else {
        searchParams.set(lteParam, lteValue);
      }

      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleStartDateRangeChange = handleDateRangeChange(START_DATE_GTE_PARAM, START_DATE_LTE_PARAM);
  const handleEndDateRangeChange = handleDateRangeChange(END_DATE_GTE_PARAM, END_DATE_LTE_PARAM);
  const handleRunAfterRangeChange = handleDateRangeChange(RUN_AFTER_GTE_PARAM, RUN_AFTER_LTE_PARAM);

  return (
    <>
      <HStack paddingY="4px">
        {!Boolean(dagId) && (
          <Box>
            <SearchBar
              defaultValue={filteredDagId ?? ""}
              hideAdvanced
              hotkeyDisabled={false}
              onChange={handleDagIdChange}
              placeHolder={translate("dags:filters.dagIdPatternFilter")}
            />
          </Box>
        )}
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
          <Select.Trigger colorPalette="blue" isActive={Boolean(filteredState)} minW="max-content">
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
          <Select.Trigger colorPalette="blue" isActive={Boolean(filteredType)} minW="max-content">
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
      <HStack flexWrap="wrap" gap={6} paddingY="4px">
        {/* Start Date Range */}
        <Box>
          <HStack alignItems="flex-start">
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.startDateFromPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleStartDateRangeChange(event.target.value, startDateLte ?? "")}
                size="sm"
                value={startDateGte ?? ""}
              />
            </Box>
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.startDateToPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleStartDateRangeChange(startDateGte ?? "", event.target.value)}
                size="sm"
                value={startDateLte ?? ""}
              />
            </Box>
          </HStack>
        </Box>
        {/* End Date Range */}
        <Box>
          <HStack alignItems="flex-start">
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.endDateFromPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleEndDateRangeChange(event.target.value, endDateLte ?? "")}
                size="sm"
                value={endDateGte ?? ""}
              />
            </Box>
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.endDateToPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleEndDateRangeChange(endDateGte ?? "", event.target.value)}
                size="sm"
                value={endDateLte ?? ""}
              />
            </Box>
          </HStack>
        </Box>
        {/* Run After Range */}
        <Box>
          <HStack alignItems="flex-start">
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.runAfterFromPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleRunAfterRangeChange(event.target.value, runAfterLte ?? "")}
                size="sm"
                value={runAfterGte ?? ""}
              />
            </Box>
            <Box w="180px">
              <Box marginBottom={1} minHeight="1.2em">
                <Text fontSize="xs">{translate("common:filters.runAfterToPlaceholder")}</Text>
              </Box>
              <DateTimeInput
                onChange={(event) => handleRunAfterRangeChange(runAfterGte ?? "", event.target.value)}
                size="sm"
                value={runAfterLte ?? ""}
              />
            </Box>
          </HStack>
        </Box>
        {/* Clear Filters Button */}
        <Box alignItems="flex-end" display="flex" height="100%">
          <Button
            _hover={{ bg: "red.600" }}
            bg="red.700"
            borderColor="red.500"
            borderRadius="md"
            color="white"
            ml={2}
            onClick={() => {
              searchParams.forEach((_, key) => searchParams.delete(key));
              setTableURLState({
                pagination: { ...pagination, pageIndex: 0 },
                sorting: [],
              });
              setSearchParams(searchParams);
            }}
            px={3}
            py={1.5}
            type="button"
          >
            {translate("common:filters.clearAllFilters", "Clear Filters")}
          </Button>
        </Box>
      </HStack>

      <DataTable
        columns={runColumns(translate, dagId)}
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
