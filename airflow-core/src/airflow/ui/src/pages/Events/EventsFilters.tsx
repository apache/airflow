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
import { Box, HStack, Input } from "@chakra-ui/react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys } from "src/constants/searchParams";

import { ResetButton } from "./ResetButton";
import { getFilterCount, formatDateTimeLocalValue } from "./filterUtils";

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

export const EventsFilters = () => {
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

  // Extract filter values from URL
  const afterFilter = searchParams.get(AFTER_PARAM) ?? "";
  const beforeFilter = searchParams.get(BEFORE_PARAM) ?? "";
  const dagIdFilter = searchParams.get(DAG_ID_PARAM) ?? "";
  const eventTypeFilter = searchParams.get(EVENT_TYPE_PARAM) ?? "";
  const mapIndexFilter = searchParams.get(MAP_INDEX_PARAM) ?? "";
  const runIdFilter = searchParams.get(RUN_ID_PARAM) ?? "";
  const taskIdFilter = searchParams.get(TASK_ID_PARAM) ?? "";
  const tryNumberFilter = searchParams.get(TRY_NUMBER_PARAM) ?? "";
  const userFilter = searchParams.get(USER_PARAM) ?? "";

  const handleInputChange = useCallback(
    (paramName: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
      const { value } = event.target;

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

  const handleDateTimeChange = useCallback(
    (paramName: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
      const { value } = event.target;

      if (value) {
        // Convert datetime-local value to ISO string
        try {
          const date = new Date(value);

          if (!isNaN(date.getTime())) {
            searchParams.set(paramName, date.toISOString());
          }
        } catch {
          // Invalid date, remove the parameter
          searchParams.delete(paramName);
        }
      } else {
        searchParams.delete(paramName);
      }
      resetPagination();
      setSearchParams(searchParams);
    },
    [resetPagination, searchParams, setSearchParams],
  );

  const handleClearFilters = useCallback(() => {
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
    <HStack justifyContent="space-between">
      <HStack flexWrap="wrap" gap={4}>
        {/* Timestamp Range Filters */}
        <Input
          onChange={handleDateTimeChange(AFTER_PARAM)}
          placeholder={translate("auditLog.filters.startDateTime")}
          size="sm"
          type="datetime-local"
          value={formatDateTimeLocalValue(afterFilter)}
          width="200px"
        />
        <Input
          onChange={handleDateTimeChange(BEFORE_PARAM)}
          placeholder={translate("auditLog.filters.endDateTime")}
          size="sm"
          type="datetime-local"
          value={formatDateTimeLocalValue(beforeFilter)}
          width="200px"
        />

        {/* Event Type Filter */}
        <Input
          onChange={handleInputChange(EVENT_TYPE_PARAM)}
          placeholder={translate("auditLog.filters.eventType")}
          size="sm"
          value={eventTypeFilter}
          width="150px"
        />

        {/* User Filter */}
        <Input
          onChange={handleInputChange(USER_PARAM)}
          placeholder={translate("common:user")}
          size="sm"
          value={userFilter}
          width="150px"
        />

        {/* DAG ID Filter */}
        <Input
          onChange={handleInputChange(DAG_ID_PARAM)}
          placeholder={translate("common:dagId")}
          size="sm"
          value={dagIdFilter}
          width="150px"
        />

        {/* Task ID Filter */}
        <Input
          onChange={handleInputChange(TASK_ID_PARAM)}
          placeholder={translate("common:taskId")}
          size="sm"
          value={taskIdFilter}
          width="150px"
        />

        {/* Run ID Filter */}
        <Input
          onChange={handleInputChange(RUN_ID_PARAM)}
          placeholder={translate("common:runId")}
          size="sm"
          value={runIdFilter}
          width="150px"
        />

        {/* Map Index Filter */}
        <Input
          onChange={handleInputChange(MAP_INDEX_PARAM)}
          placeholder={translate("common:mapIndex")}
          size="sm"
          type="number"
          value={mapIndexFilter}
          width="120px"
        />

        {/* Try Number Filter */}
        <Input
          onChange={handleInputChange(TRY_NUMBER_PARAM)}
          placeholder={translate("common:tryNumber")}
          size="sm"
          type="number"
          value={tryNumberFilter}
          width="120px"
        />
      </HStack>

      <Box>
        <ResetButton filterCount={filterCount} onClearFilters={handleClearFilters} />
      </Box>
    </HStack>
  );
};
