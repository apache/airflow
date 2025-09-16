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
import type { FilterValue } from "src/components/FilterBar";
import { useFilterConfigs } from "src/constants/filterConfigs";
import type { SearchParamsKeys } from "src/constants/searchParams";

export type FilterableSearchParamsKeys =
  | SearchParamsKeys.AFTER
  | SearchParamsKeys.BEFORE
  | SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN
  | SearchParamsKeys.DAG_ID
  | SearchParamsKeys.END_DATE
  | SearchParamsKeys.EVENT_TYPE
  | SearchParamsKeys.KEY_PATTERN
  | SearchParamsKeys.LOGICAL_DATE_GTE
  | SearchParamsKeys.LOGICAL_DATE_LTE
  | SearchParamsKeys.MAP_INDEX
  | SearchParamsKeys.RUN_AFTER_GTE
  | SearchParamsKeys.RUN_AFTER_LTE
  | SearchParamsKeys.RUN_ID
  | SearchParamsKeys.RUN_ID_PATTERN
  | SearchParamsKeys.START_DATE
  | SearchParamsKeys.TASK_ID
  | SearchParamsKeys.TASK_ID_PATTERN
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
      filterConfigs.forEach((config) => {
        const value = filters[config.key];

        if (value === null || value === undefined || value === "") {
          searchParams.delete(config.key);
        } else {
          searchParams.set(config.key, String(value));
        }
      });

      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [filterConfigs, pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  return {
    filterConfigs,
    handleFiltersChange,
    searchParams,
  };
};
