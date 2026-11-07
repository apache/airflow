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
import { FilterBar, type DateRangeValue, type FilterValue } from "src/components/FilterBar";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

const hasWhenRange = (value: FilterValue): boolean => {
  if (value === null || value === undefined || typeof value !== "object" || value instanceof Date) {
    return false;
  }

  if (Array.isArray(value)) {
    return false;
  }

  const range = value as DateRangeValue;

  return (range.startDate ?? "") !== "" || (range.endDate ?? "") !== "";
};

// Missing `after` is also the state before the default week is written into the URL.
// Recording the clear here, in the same params update, stops that default being applied again.
const rememberClearedWindow = (params: URLSearchParams, filters: Record<string, FilterValue>) => {
  if (hasWhenRange(filters[SearchParamsKeys.EVENT_DATE_RANGE])) {
    params.delete(SearchParamsKeys.ALL_EVENTS);

    return;
  }

  params.set(SearchParamsKeys.ALL_EVENTS, "1");
};

type EventsFiltersProps = {
  readonly urlDagId?: string;
  readonly urlRunId?: string;
  readonly urlTaskId?: string;
};

export const EventsFilters = ({ urlDagId, urlRunId, urlTaskId }: EventsFiltersProps) => {
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.EVENT_DATE_RANGE,
    SearchParamsKeys.EVENT_TYPE,
    SearchParamsKeys.USER,
    SearchParamsKeys.MAP_INDEX,
    SearchParamsKeys.TRY_NUMBER,
  ];

  if (multiTeamEnabled) {
    searchParamKeys.push(SearchParamsKeys.TEAMS);
  }

  // Only add Dag ID filter if not in URL context
  if (urlDagId === undefined) {
    searchParamKeys.push(SearchParamsKeys.DAG_ID);
  }

  // Only add Run ID filter if not in URL context
  if (urlRunId === undefined) {
    searchParamKeys.push(SearchParamsKeys.RUN_ID);
  }

  // Only add Task ID filter if not in URL context
  if (urlTaskId === undefined) {
    searchParamKeys.push(SearchParamsKeys.TASK_ID);
  }

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(
    searchParamKeys,
    // Dag, run, and task audit tabs are already scoped and keep the full history.
    urlDagId === undefined ? { adjustParams: rememberClearedWindow } : undefined,
  );

  return (
    <FilterBar configs={filterConfigs} initialValues={initialValues} onFiltersChange={handleFiltersChange} />
  );
};
