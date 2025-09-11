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
import { Box, HStack, VStack, Text } from "@chakra-ui/react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchBar } from "src/components/SearchBar";
import { ResetButton } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { getFilterCount } from "src/utils/filterUtils";

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
} = SearchParamsKeys;

type EventsFiltersProps = {
  readonly urlDagId?: string;
  readonly urlRunId?: string;
  readonly urlTaskId?: string;
};

export const EventsFilters = ({ urlDagId, urlRunId, urlTaskId }: EventsFiltersProps) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();

  const { pagination, sorting } = tableURLState;

  const resetPagination = useCallback(() => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
  }, [pagination, setTableURLState, sorting]);

  const afterFilter = searchParams.get(AFTER_PARAM);
  const beforeFilter = searchParams.get(BEFORE_PARAM);
  const dagIdFilter = searchParams.get(DAG_ID_PARAM);
  const eventTypeFilter = searchParams.get(EVENT_TYPE_PARAM);
  const mapIndexFilter = searchParams.get(MAP_INDEX_PARAM);
  const runIdFilter = searchParams.get(RUN_ID_PARAM);
  const taskIdFilter = searchParams.get(TASK_ID_PARAM);
  const tryNumberFilter = searchParams.get(TRY_NUMBER_PARAM);
  const userFilter = searchParams.get(USER_PARAM);

  const updateSearchParams = useCallback(
    (paramName: string, value: string) => {
      if (value) {
        searchParams.set(paramName, value);
      } else {
        searchParams.delete(paramName);
      }
      resetPagination();
      setSearchParams(searchParams);
    },
    [resetPagination, searchParams, setSearchParams],
  );

  const onClearFilters = useCallback(() => {
    searchParams.delete(AFTER_PARAM);
    searchParams.delete(BEFORE_PARAM);
    searchParams.delete(DAG_ID_PARAM);
    searchParams.delete(EVENT_TYPE_PARAM);
    searchParams.delete(MAP_INDEX_PARAM);
    searchParams.delete(RUN_ID_PARAM);
    searchParams.delete(TASK_ID_PARAM);
    searchParams.delete(TRY_NUMBER_PARAM);
    searchParams.delete(USER_PARAM);
    resetPagination();
    setSearchParams(searchParams);
  }, [resetPagination, searchParams, setSearchParams]);

  const handleSearchChange = useCallback(
    (paramName: string) => (value: string) => {
      updateSearchParams(paramName, value);
    },
    [updateSearchParams],
  );

  const handleDateTimeChange = useCallback(
    (paramName: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
      const { value } = event.target;

      updateSearchParams(paramName, value);
    },
    [updateSearchParams],
  );

  const filterCount = getFilterCount({
    after: afterFilter,
    before: beforeFilter,
    dagId: dagIdFilter,
    eventType: eventTypeFilter,
    mapIndex: mapIndexFilter,
    runId: runIdFilter,
    taskId: taskIdFilter,
    tryNumber: tryNumberFilter,
    user: userFilter,
  });

  return (
    <HStack alignItems="end" justifyContent="space-between">
      <HStack alignItems="end" gap={4}>
        {/* Timestamp Range Filters */}
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs" fontWeight="medium">
            {translate("common:table.from")}
          </Text>
          <DateTimeInput
            onChange={handleDateTimeChange(AFTER_PARAM)}
            placeholder={translate("common:startDate")}
            size="sm"
            value={afterFilter ?? ""}
          />
        </VStack>
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs" fontWeight="medium">
            {translate("common:table.to")}
          </Text>
          <DateTimeInput
            onChange={handleDateTimeChange(BEFORE_PARAM)}
            placeholder={translate("common:endDate")}
            size="sm"
            value={beforeFilter ?? ""}
          />
        </VStack>

        {/* Event Type Filter */}
        <Box>
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={eventTypeFilter ?? ""}
            hideAdvanced={true}
            hotkeyDisabled={true}
            onChange={handleSearchChange(EVENT_TYPE_PARAM)}
            placeHolder={translate("auditLog.filters.eventType")}
          />
        </Box>

        {/* User Filter */}
        <Box>
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={userFilter ?? ""}
            hideAdvanced={true}
            hotkeyDisabled={true}
            onChange={handleSearchChange(USER_PARAM)}
            placeHolder={translate("common:user")}
          />
        </Box>

        {/* DAG ID Filter - Hide if URL already has dagId */}
        {urlDagId === undefined && (
          <Box>
            <SearchBar
              buttonProps={{ disabled: true }}
              defaultValue={dagIdFilter ?? ""}
              hideAdvanced={true}
              hotkeyDisabled={true}
              onChange={handleSearchChange(DAG_ID_PARAM)}
              placeHolder={translate("common:dagId")}
            />
          </Box>
        )}

        {/* Task ID Filter - Hide if URL already has taskId */}
        {urlTaskId === undefined && (
          <Box>
            <SearchBar
              buttonProps={{ disabled: true }}
              defaultValue={taskIdFilter ?? ""}
              hideAdvanced={true}
              hotkeyDisabled={true}
              onChange={handleSearchChange(TASK_ID_PARAM)}
              placeHolder={translate("common:taskId")}
            />
          </Box>
        )}

        {/* Run ID Filter - Hide if URL already has runId */}
        {urlRunId === undefined && (
          <Box>
            <SearchBar
              buttonProps={{ disabled: true }}
              defaultValue={runIdFilter ?? ""}
              hideAdvanced={true}
              hotkeyDisabled={true}
              onChange={handleSearchChange(RUN_ID_PARAM)}
              placeHolder={translate("common:runId")}
            />
          </Box>
        )}

        {/* Map Index Filter */}
        <Box>
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={mapIndexFilter ?? ""}
            hideAdvanced={true}
            hotkeyDisabled={true}
            onChange={handleSearchChange(MAP_INDEX_PARAM)}
            placeHolder={translate("common:mapIndex")}
          />
        </Box>

        {/* Try Number Filter */}
        <Box>
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={tryNumberFilter ?? ""}
            hideAdvanced={true}
            hotkeyDisabled={true}
            onChange={handleSearchChange(TRY_NUMBER_PARAM)}
            placeHolder={translate("common:tryNumber")}
          />
        </Box>
      </HStack>

      <Box>
        <ResetButton filterCount={filterCount} onClearFilters={onClearFilters} />
      </Box>
    </HStack>
  );
};
