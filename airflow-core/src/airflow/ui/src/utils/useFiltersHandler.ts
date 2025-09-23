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
import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import type { FilterValue, DateRangeValue } from "src/components/FilterBar";
import { useFilterConfigs } from "src/constants/filterConfigs";
import { SearchParamsKeys } from "src/constants/searchParams";

const isNonEmptyString = (value: string | null | undefined): value is string =>
  value !== null && value !== undefined && value !== "";

const isValidDateRangeValue = (value: DateRangeValue): boolean =>
  isNonEmptyString(value.startDate) || isNonEmptyString(value.endDate);

const handleLogicalDateRange = (newParams: URLSearchParams, rangeValue: DateRangeValue) => {
  newParams.delete(SearchParamsKeys.LOGICAL_DATE_RANGE as string);
  if (isNonEmptyString(rangeValue.startDate)) {
    newParams.set(SearchParamsKeys.LOGICAL_DATE_GTE, rangeValue.startDate);
  } else {
    newParams.delete(SearchParamsKeys.LOGICAL_DATE_GTE);
  }
  if (isNonEmptyString(rangeValue.endDate)) {
    newParams.set(SearchParamsKeys.LOGICAL_DATE_LTE, rangeValue.endDate);
  } else {
    newParams.delete(SearchParamsKeys.LOGICAL_DATE_LTE);
  }
};

const handleRunAfterRange = (newParams: URLSearchParams, rangeValue: DateRangeValue) => {
  newParams.delete(SearchParamsKeys.RUN_AFTER_RANGE as string);
  if (isNonEmptyString(rangeValue.startDate)) {
    newParams.set(SearchParamsKeys.RUN_AFTER_GTE, rangeValue.startDate);
  } else {
    newParams.delete(SearchParamsKeys.RUN_AFTER_GTE);
  }
  if (isNonEmptyString(rangeValue.endDate)) {
    newParams.set(SearchParamsKeys.RUN_AFTER_LTE, rangeValue.endDate);
  } else {
    newParams.delete(SearchParamsKeys.RUN_AFTER_LTE);
  }
};

export type FilterableSearchParamsKeys =
  | SearchParamsKeys.AFTER
  | SearchParamsKeys.BEFORE
  | SearchParamsKeys.BODY_SEARCH
  | SearchParamsKeys.CONF_CONTAINS
  | SearchParamsKeys.CREATED_AT_GTE
  | SearchParamsKeys.CREATED_AT_LTE
  | SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN
  | SearchParamsKeys.DAG_ID
  | SearchParamsKeys.DAG_ID_PATTERN
  | SearchParamsKeys.DAG_VERSION
  | SearchParamsKeys.DURATION_GTE
  | SearchParamsKeys.DURATION_LTE
  | SearchParamsKeys.END_DATE
  | SearchParamsKeys.EVENT_TYPE
  | SearchParamsKeys.KEY_PATTERN
  | SearchParamsKeys.LOGICAL_DATE_GTE
  | SearchParamsKeys.LOGICAL_DATE_LTE
  | SearchParamsKeys.LOGICAL_DATE_RANGE
  | SearchParamsKeys.MAP_INDEX
  | SearchParamsKeys.RESPONDED_BY_USER_NAME
  | SearchParamsKeys.RESPONSE_RECEIVED
  | SearchParamsKeys.RUN_AFTER_GTE
  | SearchParamsKeys.RUN_AFTER_LTE
  | SearchParamsKeys.RUN_AFTER_RANGE
  | SearchParamsKeys.RUN_ID
  | SearchParamsKeys.RUN_ID_PATTERN
  | SearchParamsKeys.RUN_TYPE
  | SearchParamsKeys.START_DATE
  | SearchParamsKeys.STATE
  | SearchParamsKeys.SUBJECT_SEARCH
  | SearchParamsKeys.TASK_ID
  | SearchParamsKeys.TASK_ID_PATTERN
  | SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN
  | SearchParamsKeys.TRY_NUMBER
  | SearchParamsKeys.USER;

export const useFiltersHandler = (searchParamKeys: Array<FilterableSearchParamsKeys>) => {
  const { getFilterConfig } = useFilterConfigs();

  const filterConfigs = useMemo(
    () => searchParamKeys.map((key) => getFilterConfig(key)),
    [searchParamKeys, getFilterConfig],
  );
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handleFiltersChange = useCallback(
    (filters: Record<string, FilterValue>) => {
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });

      setSearchParams((prevParams) => {
        const newParams = new URLSearchParams(prevParams);

        filterConfigs.forEach((config) => {
          const value = filters[config.key];

          if (value === null || value === undefined || value === "") {
            newParams.delete(config.key);
            if (config.key === (SearchParamsKeys.LOGICAL_DATE_RANGE as string)) {
              newParams.delete(SearchParamsKeys.LOGICAL_DATE_GTE);
              newParams.delete(SearchParamsKeys.LOGICAL_DATE_LTE);
            }
            if (config.key === (SearchParamsKeys.RUN_AFTER_RANGE as string)) {
              newParams.delete(SearchParamsKeys.RUN_AFTER_GTE);
              newParams.delete(SearchParamsKeys.RUN_AFTER_LTE);
            }
          } else if (config.type === "daterange" && typeof value === "object") {
            const rangeValue = value as DateRangeValue;

            if (isValidDateRangeValue(rangeValue)) {
              if (config.key === (SearchParamsKeys.LOGICAL_DATE_RANGE as string)) {
                handleLogicalDateRange(newParams, rangeValue);
              } else if (config.key === (SearchParamsKeys.RUN_AFTER_RANGE as string)) {
                handleRunAfterRange(newParams, rangeValue);
              } else {
                newParams.set(config.key, JSON.stringify(rangeValue));
              }
            } else {
              newParams.delete(config.key);
              if (config.key === (SearchParamsKeys.LOGICAL_DATE_RANGE as string)) {
                newParams.delete(SearchParamsKeys.LOGICAL_DATE_GTE);
                newParams.delete(SearchParamsKeys.LOGICAL_DATE_LTE);
              }
            }
          } else {
            newParams.set(config.key, typeof value === "object" ? JSON.stringify(value) : String(value));
          }
        });

        newParams.delete(SearchParamsKeys.OFFSET);

        return newParams;
      });
    },
    [filterConfigs, pagination, setSearchParams, setTableURLState, sorting],
  );

  return {
    filterConfigs,
    handleFiltersChange,
    searchParams,
  };
};
