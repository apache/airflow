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
import { Box, HStack, Input, VStack, Text } from "@chakra-ui/react";
import { useCallback, useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchParamsKeys } from "src/constants/searchParams";

import { ResetButton } from "./ResetButton";
import { getFilterCount } from "./filterUtils";

const debounceDelay = 300; // Debounce delay for filter inputs

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
  readonly onClearFilters: () => void;
  readonly urlDagId?: string;
  readonly urlRunId?: string;
  readonly urlTaskId?: string;
};

export const EventsFilters = ({ onClearFilters, urlDagId, urlRunId, urlTaskId }: EventsFiltersProps) => {
  const { t: translate } = useTranslation(["browse", "common", "components"]);
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

  // Local state - for immediate UI feedback
  const [localAfterFilter, setLocalAfterFilter] = useState(afterFilter ?? "");
  const [localBeforeFilter, setLocalBeforeFilter] = useState(beforeFilter ?? "");
  const [localDagIdFilter, setLocalDagIdFilter] = useState(dagIdFilter ?? "");
  const [localEventTypeFilter, setLocalEventTypeFilter] = useState(eventTypeFilter ?? "");
  const [localMapIndexFilter, setLocalMapIndexFilter] = useState(mapIndexFilter ?? "");
  const [localRunIdFilter, setLocalRunIdFilter] = useState(runIdFilter ?? "");
  const [localTaskIdFilter, setLocalTaskIdFilter] = useState(taskIdFilter ?? "");
  const [localTryNumberFilter, setLocalTryNumberFilter] = useState(tryNumberFilter ?? "");
  const [localUserFilter, setLocalUserFilter] = useState(userFilter ?? "");

  // Sync local state with URL changes (e.g., from clear filters)
  useEffect(() => setLocalAfterFilter(afterFilter ?? ""), [afterFilter]);
  useEffect(() => setLocalBeforeFilter(beforeFilter ?? ""), [beforeFilter]);
  useEffect(() => setLocalDagIdFilter(dagIdFilter ?? ""), [dagIdFilter]);
  useEffect(() => setLocalEventTypeFilter(eventTypeFilter ?? ""), [eventTypeFilter]);
  useEffect(() => setLocalMapIndexFilter(mapIndexFilter ?? ""), [mapIndexFilter]);
  useEffect(() => setLocalRunIdFilter(runIdFilter ?? ""), [runIdFilter]);
  useEffect(() => setLocalTaskIdFilter(taskIdFilter ?? ""), [taskIdFilter]);
  useEffect(() => setLocalTryNumberFilter(tryNumberFilter ?? ""), [tryNumberFilter]);
  useEffect(() => setLocalUserFilter(userFilter ?? ""), [userFilter]);

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

  const debouncedUpdateSearchParams = useDebouncedCallback(updateSearchParams, debounceDelay);

  const handleInputChange = useCallback(
    (paramName: string, setLocalValue: (value: string) => void) =>
      (event: React.ChangeEvent<HTMLInputElement>) => {
        const { value } = event.target;

        // Immediate UI update
        setLocalValue(value);
        // Debounced API call
        debouncedUpdateSearchParams(paramName, value);
      },
    [debouncedUpdateSearchParams],
  );

  const handleDateTimeChange = useCallback(
    (paramName: string, setLocalValue: (value: string) => void) =>
      (event: React.ChangeEvent<HTMLInputElement>) => {
        const { value } = event.target;

        // Immediate UI update
        setLocalValue(value);
        // Debounced API call
        debouncedUpdateSearchParams(paramName, value);
      },
    [debouncedUpdateSearchParams],
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
    <HStack justifyContent="space-between">
      <HStack alignItems="end" gap={4}>
        {/* Timestamp Range Filters */}
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs" fontWeight="medium">
            {translate("components:backfill.dateRangeFrom")}
          </Text>
          <DateTimeInput
            onChange={handleDateTimeChange(AFTER_PARAM, setLocalAfterFilter)}
            placeholder={translate("auditLog.filters.startDateTime")}
            size="sm"
            value={localAfterFilter}
          />
        </VStack>
        <VStack alignItems="flex-start" gap={1}>
          <Text fontSize="xs" fontWeight="medium">
            {translate("components:backfill.dateRangeTo")}
          </Text>
          <DateTimeInput
            onChange={handleDateTimeChange(BEFORE_PARAM, setLocalBeforeFilter)}
            placeholder={translate("auditLog.filters.endDateTime")}
            size="sm"
            value={localBeforeFilter}
          />
        </VStack>

        {/* Event Type Filter */}
        <Input
          onChange={handleInputChange(EVENT_TYPE_PARAM, setLocalEventTypeFilter)}
          placeholder={translate("auditLog.filters.eventType")}
          size="sm"
          value={localEventTypeFilter}
        />

        {/* User Filter */}
        <Input
          onChange={handleInputChange(USER_PARAM, setLocalUserFilter)}
          placeholder={translate("common:user")}
          size="sm"
          value={localUserFilter}
        />

        {/* DAG ID Filter - Hide if URL already has dagId */}
        {urlDagId === undefined && (
          <Input
            onChange={handleInputChange(DAG_ID_PARAM, setLocalDagIdFilter)}
            placeholder={translate("common:dagId")}
            size="sm"
            value={localDagIdFilter}
          />
        )}

        {/* Task ID Filter - Hide if URL already has taskId */}
        {urlTaskId === undefined && (
          <Input
            onChange={handleInputChange(TASK_ID_PARAM, setLocalTaskIdFilter)}
            placeholder={translate("common:taskId")}
            size="sm"
            value={localTaskIdFilter}
          />
        )}

        {/* Run ID Filter - Hide if URL already has runId */}
        {urlRunId === undefined && (
          <Input
            onChange={handleInputChange(RUN_ID_PARAM, setLocalRunIdFilter)}
            placeholder={translate("common:runId")}
            size="sm"
            value={localRunIdFilter}
          />
        )}

        {/* Map Index Filter */}
        <Input
          onChange={handleInputChange(MAP_INDEX_PARAM, setLocalMapIndexFilter)}
          placeholder={translate("common:mapIndex")}
          size="sm"
          type="number"
          value={localMapIndexFilter}
        />

        {/* Try Number Filter */}
        <Input
          onChange={handleInputChange(TRY_NUMBER_PARAM, setLocalTryNumberFilter)}
          placeholder={translate("common:tryNumber")}
          size="sm"
          type="number"
          value={localTryNumberFilter}
        />
      </HStack>

      <Box>
        <ResetButton filterCount={filterCount} onClearFilters={onClearFilters} />
      </Box>
    </HStack>
  );
};
