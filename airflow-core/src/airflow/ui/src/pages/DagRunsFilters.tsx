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
import { VStack } from "@chakra-ui/react";
import { useMemo } from "react";

import { FilterBar, type FilterValue } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

type DagRunsFiltersProps = {
  readonly dagId?: string;
};

export const DagRunsFilters = ({ dagId }: DagRunsFiltersProps) => {
  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      SearchParamsKeys.RUN_ID_PATTERN,
      SearchParamsKeys.STATE,
      SearchParamsKeys.RUN_TYPE,
      SearchParamsKeys.START_DATE,
      SearchParamsKeys.END_DATE,
      SearchParamsKeys.RUN_AFTER_GTE,
      SearchParamsKeys.RUN_AFTER_LTE,
      SearchParamsKeys.DURATION_GTE,
      SearchParamsKeys.DURATION_LTE,
      SearchParamsKeys.CONF_CONTAINS,
      SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN,
      SearchParamsKeys.DAG_VERSION,
    ];

    if (dagId === undefined) {
      keys.unshift(SearchParamsKeys.DAG_ID_PATTERN);
    }

    return keys;
  }, [dagId]);

  const { filterConfigs, handleFiltersChange, searchParams } = useFiltersHandler(searchParamKeys);

  const initialValues = useMemo(() => {
    const values: Record<string, FilterValue> = {};

    filterConfigs.forEach((config) => {
      const value = searchParams.get(config.key);

      if (value !== null && value !== "") {
        if (config.type === "number") {
          const parsedValue = Number(value);

          values[config.key] = isNaN(parsedValue) ? value : parsedValue;
        } else {
          values[config.key] = value;
        }
      }
    });

    return values;
  }, [searchParams, filterConfigs]);

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={handleFiltersChange}
      />
    </VStack>
  );
};
