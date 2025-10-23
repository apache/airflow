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
import { HStack, VStack } from "@chakra-ui/react";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams, useParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { FilterBar, type FilterValue } from "src/components/FilterBar";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";


const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  DAG_VERSION: DAG_VERSION_PARAM,
  DURATION_GTE: DURATION_GTE_PARAM,
  DURATION_LTE: DURATION_LTE_PARAM,
  END_DATE: END_DATE_PARAM,
  LOGICAL_DATE_GTE: LOGICAL_DATE_GTE_PARAM,
  LOGICAL_DATE_LTE: LOGICAL_DATE_LTE_PARAM,
  MAP_INDEX: MAP_INDEX_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  OPERATOR: OPERATOR_PARAM,
  POOL: POOL_PARAM,
  QUEUE: QUEUE_PARAM,
  RUN_ID: RUN_ID_PARAM,
  START_DATE: START_DATE_PARAM,
  TASK_STATE: STATE_PARAM,
  TRY_NUMBER: TRY_NUMBER_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

type Props = {
  readonly setTaskDisplayNamePattern: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly taskDisplayNamePattern: string | undefined;
};

export const TaskInstancesFilter = ({
  setTaskDisplayNamePattern,
  taskDisplayNamePattern,
}: Props) => {
  const { dagId, runId } = useParams();
  const paramKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      LOGICAL_DATE_GTE_PARAM as FilterableSearchParamsKeys,
      LOGICAL_DATE_LTE_PARAM as FilterableSearchParamsKeys,
      START_DATE_PARAM as FilterableSearchParamsKeys,
      END_DATE_PARAM as FilterableSearchParamsKeys,
      DURATION_GTE_PARAM as FilterableSearchParamsKeys,
      DURATION_LTE_PARAM as FilterableSearchParamsKeys,
      TRY_NUMBER_PARAM as FilterableSearchParamsKeys,
      MAP_INDEX_PARAM as FilterableSearchParamsKeys,
      DAG_VERSION_PARAM as FilterableSearchParamsKeys,
      OPERATOR_PARAM as FilterableSearchParamsKeys,
      POOL_PARAM as FilterableSearchParamsKeys,
      QUEUE_PARAM as FilterableSearchParamsKeys,
      STATE_PARAM as FilterableSearchParamsKeys,
    ];

    if (runId === undefined) {
      keys.splice(1, 0, RUN_ID_PARAM as FilterableSearchParamsKeys);
    }

    return keys;
  }, [runId]);

  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation();

  const { filterConfigs, handleFiltersChange } = useFiltersHandler(paramKeys);




  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setTaskDisplayNamePattern(value);
    setSearchParams(searchParams);
  };

  const handleDagIdPatternChange = useCallback(
    (value: string) => {
      if (value === "") {
        searchParams.delete(DAG_ID_PATTERN_PARAM);
      } else {
        searchParams.set(DAG_ID_PATTERN_PARAM, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );


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
      <HStack alignItems="start" minWidth="100%" paddingY="4px">
        {dagId === undefined && (
          <SearchBar
            buttonProps={{ disabled: true }}
            defaultValue={filteredDagIdPattern ?? ""}
            hideAdvanced
            hotkeyDisabled={true}
            onChange={handleDagIdPatternChange}
            placeHolder={translate("dags:search.dags")}
          />
        )}
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={taskDisplayNamePattern ?? ""}
          hideAdvanced
          hotkeyDisabled={Boolean(runId)}
          onChange={handleSearchChange}
          placeHolder={translate("dags:search.tasks")}
        />
      </HStack>
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
