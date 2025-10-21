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
import { useParams, useSearchParams } from "react-router-dom";

import { FilterBar, type FilterValue } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

export const HITLFilters = ({ onResponseChange }: { readonly onResponseChange: () => void }) => {
  const { dagId = "~", taskId = "~" } = useParams();
  const [urlSearchParams] = useSearchParams();

  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const fixedKeys: Array<FilterableSearchParamsKeys> = [
      SearchParamsKeys.RESPONSE_RECEIVED,
      SearchParamsKeys.RESPONDED_BY_USER_NAME,
      SearchParamsKeys.MAP_INDEX,
      SearchParamsKeys.SUBJECT_SEARCH,
      SearchParamsKeys.BODY_SEARCH,
      SearchParamsKeys.CREATED_AT_GTE,
      SearchParamsKeys.CREATED_AT_LTE,
    ];

    const keys: Array<FilterableSearchParamsKeys> = [];

    if (dagId === "~") {
      keys.push(SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN);
    }

    if (taskId === "~") {
      keys.push(SearchParamsKeys.TASK_ID_PATTERN);
    }

    return [...keys, ...fixedKeys];
  }, [dagId, taskId]);

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

    searchParamKeys.forEach((key) => {
      const value = urlSearchParams.get(key);

      if (value !== null && value.trim() !== "") {
        values[key] = value;
      }
    });

    return values;
  }, [filterConfigs, searchParamKeys, searchParams, urlSearchParams]);

  return (
    <VStack align="start" pt={2}>
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={(filters) => {
          onResponseChange();
          handleFiltersChange(filters);
        }}
      />
    </VStack>
  );
};
