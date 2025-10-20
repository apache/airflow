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
import { HStack, VStack, type SelectValueChangeDetails, Box } from "@chakra-ui/react";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams, useParams } from "react-router-dom";

import { FilterBar, type FilterValue } from "src/components/FilterBar";

import type { TaskInstanceCollectionResponse } from "openapi/requests";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";
import { ResetButton } from "src/components/ui";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { AttrSelectFilterMulti } from "./AttrSelectFilterMulti";
import { StateFilter } from "./StateFilter";

const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  OPERATOR: OPERATOR_PARAM,
  POOL: POOL_PARAM,
  QUEUE: QUEUE_PARAM,
  STATE: STATE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

type Props = {
  readonly instances?: TaskInstanceCollectionResponse | undefined;
  readonly setTaskDisplayNamePattern: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly taskDisplayNamePattern: string | undefined;
};

export const TaskInstancesFilter = ({
  instances,
  setTaskDisplayNamePattern,
  taskDisplayNamePattern,
}: Props) => {
  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      SearchParamsKeys.LOGICAL_DATE_GTE,
      SearchParamsKeys.LOGICAL_DATE_LTE,
      SearchParamsKeys.START_DATE,
      SearchParamsKeys.END_DATE,
      SearchParamsKeys.DURATION_GTE,
      SearchParamsKeys.DURATION_LTE,
      SearchParamsKeys.TRY_NUMBER,
      SearchParamsKeys.MAP_INDEX,
      SearchParamsKeys.DAG_VERSION,
    ];

    if (runId === undefined) {
      keys.splice(1, 0, SearchParamsKeys.RUN_ID);
    }



    return keys;
  }, []);
  const { dagId, runId } = useParams();

  const [ searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation();

  
  const { filterConfigs, handleFiltersChange} = useFiltersHandler(searchParamKeys);


  const uniq = (xs: Array<string | null | undefined>) =>
    [...new Set(xs.filter((x): x is string => x !== null && x !== undefined && x !== ""))];

  const resetPagination = useCallback(() => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
  }, [pagination, sorting, setTableURLState]);

  const setMultiParam = useCallback((key: string, values: Array<string>) => {
    resetPagination();
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);

      next.delete(key);
      values.forEach((val) => next.append(key, val));

      return next;
    });
  }, [resetPagination, setSearchParams]);

  const allOperatorNames: Array<string> = uniq(
    instances?.task_instances.map((ti) => ti.operator_name as string | null | undefined) ?? []
  );
  const allQueueValues: Array<string> = uniq(
    instances?.task_instances.map((ti) => ti.queue as string | null | undefined) ?? []
  );
  const allPoolValues: Array<string> = uniq(
    instances?.task_instances.map((ti) => ti.pool as string | null | undefined) ?? []
  );


 
  const filteredState = searchParams.getAll(STATE_PARAM);

  const selectedOperators = searchParams.getAll(OPERATOR_PARAM);
  const selectedQueues = searchParams.getAll(QUEUE_PARAM);
  const selectedPools = searchParams.getAll(POOL_PARAM);

  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const hasFilteredState = filteredState.length > 0;





  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.delete(STATE_PARAM);
        value.filter((state) => state !== "all").map((state) => searchParams.append(STATE_PARAM, state));
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

const handleSelectedOperators = (value: Array<string> | undefined) =>
  setMultiParam(OPERATOR_PARAM, value ?? []);

const handleSelectedQueues = (value: Array<string> | undefined) =>
  setMultiParam(QUEUE_PARAM, value ?? []);

const handleSelectedPools = (value: Array<string> | undefined) =>
  setMultiParam(POOL_PARAM, value ?? []);

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

  const onClearFilters = useCallback(() => {
    resetPagination();
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);

      next.delete(STATE_PARAM);
      next.delete(OPERATOR_PARAM);
      next.delete(QUEUE_PARAM);
      next.delete(POOL_PARAM);

      return next;
    });
  }, [setSearchParams, resetPagination]);

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

  const taskFilterCount =
    (searchParams.getAll(STATE_PARAM).length > 0 ? 1 : 0) +
    (searchParams.getAll(OPERATOR_PARAM).length > 0 ? 1 : 0) +
    (searchParams.getAll(QUEUE_PARAM).length > 0 ? 1 : 0) +
    (searchParams.getAll(POOL_PARAM).length > 0 ? 1 : 0);

  return (
    <VStack align="start" justifyContent="space-between">
    <HStack alignItems="start" paddingY="4px">
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
      <StateFilter
        onChange={handleStateChange}
        translate={translate}
        value={hasFilteredState ? filteredState : ["all"]}
      />
      </HStack>
      <HStack>
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleSelectedOperators}
          placeholderText={translate("selectOperator")}
          selectedValues={selectedOperators}
          values={allOperatorNames}
        />
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleSelectedQueues}
          placeholderText={translate("selectQueues")}
          selectedValues={selectedQueues}
          values={allQueueValues}
        />
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleSelectedPools}
          placeholderText={translate("selectPools")}
          selectedValues={selectedPools}
          values={allPoolValues}
        />
        <Box>
          <ResetButton
            filterCount={taskFilterCount}
            onClearFilters={onClearFilters}
          />
        </Box>
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
