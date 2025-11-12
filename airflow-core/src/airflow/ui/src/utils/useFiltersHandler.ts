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

const handleDateRangeChange = (
  newParams: URLSearchParams,
  rangeValue: DateRangeValue | null,
  config: { endKey?: string; key: string; startKey?: string },
) => {
  const { endKey, key, startKey } = config;

  newParams.delete(key);

  if (startKey === undefined || endKey === undefined) {
    return;
  }

  const startDate = rangeValue?.startDate;
  const endDate = rangeValue?.endDate;

  if (isNonEmptyString(startDate)) {
    newParams.set(startKey, startDate);
  } else {
    newParams.delete(startKey);
  }

  if (isNonEmptyString(endDate)) {
    newParams.set(endKey, endDate);
  } else {
    newParams.delete(endKey);
  }
};

export type FilterableSearchParamsKeys =
  | SearchParamsKeys.ASSET_EVENT_DATE_RANGE
  | SearchParamsKeys.BODY_SEARCH
  | SearchParamsKeys.CONF_CONTAINS
  | SearchParamsKeys.CREATED_AT_RANGE
  | SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN
  | SearchParamsKeys.DAG_ID
  | SearchParamsKeys.DAG_ID_PATTERN
  | SearchParamsKeys.DAG_VERSION
  | SearchParamsKeys.DURATION_GTE
  | SearchParamsKeys.DURATION_LTE
  | SearchParamsKeys.EVENT_DATE_RANGE
  | SearchParamsKeys.EVENT_TYPE
  | SearchParamsKeys.KEY_PATTERN
  | SearchParamsKeys.LOGICAL_DATE_RANGE
  | SearchParamsKeys.MAP_INDEX
  | SearchParamsKeys.NAME_PATTERN
  | SearchParamsKeys.OPERATOR_NAME_PATTERN
  | SearchParamsKeys.POOL_NAME_PATTERN
  | SearchParamsKeys.QUEUE_NAME_PATTERN
  | SearchParamsKeys.RESPONDED_BY_USER_NAME
  | SearchParamsKeys.RESPONSE_RECEIVED
  | SearchParamsKeys.RUN_AFTER_RANGE
  | SearchParamsKeys.RUN_ID
  | SearchParamsKeys.RUN_ID_PATTERN
  | SearchParamsKeys.RUN_TYPE
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

  const initialValues = useMemo(() => {
    const values: Record<string, FilterValue> = {};

    filterConfigs.forEach((config) => {
      if (config.type === "daterange") {
        // Handle daterange filters using startKey and endKey
        const startDate =
          config.startKey !== undefined && config.startKey !== ""
            ? searchParams.get(config.startKey)
            : undefined;
        const endDate =
          config.endKey !== undefined && config.endKey !== "" ? searchParams.get(config.endKey) : undefined;

        if (
          (startDate !== undefined && startDate !== null && startDate !== "") ||
          (endDate !== undefined && endDate !== null && endDate !== "")
        ) {
          values[config.key] = {
            endDate: endDate ?? undefined,
            startDate: startDate ?? undefined,
          };
        }
      } else {
        // Handle other filter types
        const value = searchParams.get(config.key);

        if (value !== null && value !== "") {
          if (config.type === "number") {
            const parsedValue = Number(value);

            values[config.key] = isNaN(parsedValue) ? value : parsedValue;
          } else {
            values[config.key] = value;
          }
        }
      }
    });

    return values;
  }, [filterConfigs, searchParams]);

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

          newParams.delete(config.key);

          if (config.type === "daterange") {
            handleDateRangeChange(newParams, value as DateRangeValue | null, config);
          } else if (value === null || value === undefined || value === "") {
            newParams.delete(config.key);
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
    initialValues,
    searchParams,
  };
};
