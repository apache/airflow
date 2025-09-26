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
import { useMemo } from "react";

import { FilterBar } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils/useFiltersHandler";

const FILTER_KEYS: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.START_DATE,
  SearchParamsKeys.END_DATE,
  SearchParamsKeys.DAG_ID,
  SearchParamsKeys.TASK_ID,
];

export const AssetEventsFilter = () => {
  const { filterConfigs, handleFiltersChange, searchParams } = useFiltersHandler(FILTER_KEYS);

  const initialValues = useMemo(() => {
    const values: Record<string, string> = {};

    FILTER_KEYS.forEach((key) => {
      const value = searchParams.get(key);

      if (value !== null && value.trim() !== "") {
        values[key] = value;
      }
    });

    return values;
  }, [searchParams]);

  return (
    <FilterBar configs={filterConfigs} initialValues={initialValues} onFiltersChange={handleFiltersChange} />
  );
};
