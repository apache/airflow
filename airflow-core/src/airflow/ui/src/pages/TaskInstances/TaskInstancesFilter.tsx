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
import { useSearchParams, useParams } from "react-router-dom";

import { FilterBar, type FilterValue } from "src/components/FilterBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

const {
  ASSET_EVENT_DATE_RANGE: ASSET_EVENT_DATE_RANGE_PARAM,
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  DAG_VERSION: DAG_VERSION_PARAM,
  DURATION_GTE: DURATION_GTE_PARAM,
  DURATION_LTE: DURATION_LTE_PARAM,
  LOGICAL_DATE_RANGE: LOGICAL_DATE_RANGE_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  OPERATOR_NAME_PATTERN: OPERATOR_NAME_PATTERN_PARAM,
  POOL_NAME_PATTERN: POOL_NAME_PATTERN_PARAM,
  QUEUE_NAME_PATTERN: QUEUE_NAME_PATTERN_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  TASK_STATE: STATE_PARAM,
  TRY_NUMBER: TRY_NUMBER_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

export const TaskInstancesFilter = () => {
  const { dagId, runId } = useParams();
  const paramKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      NAME_PATTERN_PARAM as FilterableSearchParamsKeys,
      LOGICAL_DATE_RANGE_PARAM as FilterableSearchParamsKeys,
      ASSET_EVENT_DATE_RANGE_PARAM as FilterableSearchParamsKeys,
      DURATION_GTE_PARAM as FilterableSearchParamsKeys,
      DURATION_LTE_PARAM as FilterableSearchParamsKeys,
      TRY_NUMBER_PARAM as FilterableSearchParamsKeys,
      MAP_INDEX_PARAM as FilterableSearchParamsKeys,
      DAG_VERSION_PARAM as FilterableSearchParamsKeys,
      OPERATOR_NAME_PATTERN_PARAM as FilterableSearchParamsKeys,
      POOL_NAME_PATTERN_PARAM as FilterableSearchParamsKeys,
      QUEUE_NAME_PATTERN_PARAM as FilterableSearchParamsKeys,
      STATE_PARAM as FilterableSearchParamsKeys,
    ];

    if (runId === undefined) {
      keys.unshift(RUN_ID_PATTERN_PARAM as FilterableSearchParamsKeys);
    }

    if (dagId === undefined) {
      keys.unshift(DAG_ID_PATTERN_PARAM as FilterableSearchParamsKeys);
    }

    return keys;
  }, [dagId, runId]);

  const [searchParams] = useSearchParams();

  const { filterConfigs, handleFiltersChange } = useFiltersHandler(paramKeys);

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
    <VStack align="start" justifyContent="space-between">
      <VStack alignItems="flex-start" gap={1}>
        <FilterBar
          configs={filterConfigs}
          initialValues={initialValues}
          onFiltersChange={handleFiltersChange}
        />
      </VStack>
    </VStack>
  );
};
