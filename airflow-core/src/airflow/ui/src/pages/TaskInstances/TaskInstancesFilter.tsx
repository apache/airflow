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
import { HStack, VStack, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams, useParams } from "react-router-dom";

import { FilterBar, type FilterValue } from "src/components/FilterBar";

import type { TaskInstanceCollectionResponse } from "openapi/requests";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";
import { SearchBar } from "src/components/SearchBar";
import { StateBadge } from "src/components/StateBadge";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { Select } from "src/components/ui";
import { taskInstanceStateOptions } from "src/constants/stateOptions";
import { AttrSelectFilterMulti } from "src/pages/Dag/Tasks/TaskFilters/AttrSelectFilterMulti";
import { ResetButton } from "src/components/ui";

const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
  STATE: STATE_PARAM,
  QUEUE: QUEUE_PARAM,
  POOL: POOL_PARAM,
  OPERATOR: OPERATOR_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

type Props = {
  readonly setTaskDisplayNamePattern: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly taskDisplayNamePattern: string | undefined;
  readonly instances?: TaskInstanceCollectionResponse | undefined;
};

export const TaskInstancesFilter = ({
  setTaskDisplayNamePattern,
  taskDisplayNamePattern,
  instances,
}: Props) => {
  const { dagId, runId } = useParams();
  
  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
             
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
  }, [runId, dagId]);
  
  const uniq = (xs: Array<string | null | undefined>) =>
    Array.from(new Set(xs.filter(Boolean) as string[]));

  const setMultiParam = (key: string, values: string[]) => {
    searchParams.delete(key);
    values.forEach((v) => searchParams.append(key, v));
    resetPagination();
    setSearchParams(searchParams);
  };

  const allOperatorNames: Array<string> = uniq(
    instances?.task_instances.map((ti) => (ti.operator ?? (ti as any).operator_name) as string | null | undefined) ?? []
  );
  const allQueueValues: Array<string> = uniq(
    instances?.task_instances.map((ti) => ti.queue as string | null | undefined) ?? []
  );
  const allPoolValues: Array<string> = uniq(
    instances?.task_instances.map((ti) => ti.pool as string | null | undefined) ?? []
  );

  const { filterConfigs, handleFiltersChange} = useFiltersHandler(searchParamKeys);

  const [ searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation();

  const filteredState = searchParams.getAll(STATE_PARAM);

  const selectedOperators = searchParams.getAll(OPERATOR_PARAM);
  const selectedQueues = searchParams.getAll(QUEUE_PARAM);
  const selectedPools = searchParams.getAll(POOL_PARAM);

  const filteredDagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const hasFilteredState = filteredState.length > 0;

  const resetPagination = () =>
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });

  
  

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.delete(STATE_PARAM);
        value.filter((state) => state !== "all").map((state) => searchParams.append(STATE_PARAM, state));
      }
      resetPagination();
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

const handleSelectedOperators = (value: string[] | undefined) =>
  setMultiParam(OPERATOR_PARAM, value ?? []);

const handleSelectedQueues = (value: string[] | undefined) =>
  setMultiParam(QUEUE_PARAM, value ?? []);

const handleSelectedPools = (value: string[] | undefined) =>
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
    <HStack justifyContent="space-between">
    <HStack paddingY="4px">
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
      <Select.Root
        collection={taskInstanceStateOptions}
        maxW="450px"
        multiple
        onValueChange={handleStateChange}
        value={hasFilteredState ? filteredState : ["all"]}
      >
        <Select.Trigger
          {...(hasFilteredState ? { clearable: true } : {})}
          colorPalette="brand"
          isActive={Boolean(filteredState)}
        >
          <Select.ValueText>
            {() =>
              hasFilteredState ? (
                <HStack flexWrap="wrap" fontSize="sm" gap="4px" paddingY="8px">
                  {filteredState.map((state) => (
                    <StateBadge key={state} state={state as TaskInstanceState}>
                      {translate(`common:states.${state}`)}
                    </StateBadge>
                  ))}
                </HStack>
              ) : (
                translate("dags:filters.allStates")
              )
            }
          </Select.ValueText>
        </Select.Trigger>
        <Select.Content>
          {taskInstanceStateOptions.items.map((option) => (
            <Select.Item item={option} key={option.label}>
              {option.value === "all" ? (
                translate(option.label)
              ) : (
                <StateBadge state={option.value as TaskInstanceState}>{translate(option.label)}</StateBadge>
              )}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      </HStack>
      <HStack justifyContent="space-between" paddingY="4px" gap={2}>
        <AttrSelectFilterMulti
          displayPrefix={translate("operator")}
          handleSelect={handleSelectedOperators}
          placeholderText={translate("selectOperator")}
          selectedValues={selectedOperators}
          values={allOperatorNames}
        />
        <AttrSelectFilterMulti
          displayPrefix={translate("queue")}
          handleSelect={handleSelectedQueues}
          placeholderText={translate("selectQueues")}
          selectedValues={selectedQueues}
          values={allQueueValues}
        />
        <AttrSelectFilterMulti
          displayPrefix={translate("pool")}
          handleSelect={handleSelectedPools}
          placeholderText={translate("selectPools")}
          selectedValues={selectedPools}
          values={allPoolValues}
        />
      </HStack>
      <VStack alignItems="flex-start" gap={1}>
        <FilterBar
          configs={filterConfigs}
          initialValues={initialValues}
          onFiltersChange={handleFiltersChange}
        />
      </VStack>
    </HStack>     
  );
};
  

