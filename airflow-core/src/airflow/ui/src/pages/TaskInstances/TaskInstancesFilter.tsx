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
import { HStack, type SelectValueChangeDetails, Box } from "@chakra-ui/react";
import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams, useParams } from "react-router-dom";


import { FilterBar, type FilterValue } from "src/components/FilterBar";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";


const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  NAME_PATTERN: NAME_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;


export const TaskInstancesFilter = ({
  setTaskDisplayNamePattern,
  taskDisplayNamePattern,
}: {
  readonly setTaskDisplayNamePattern: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly taskDisplayNamePattern: string | undefined;
}) => {
  const { dagId, runId } = useParams();
  
  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      SearchParamsKeys.TASK_STATE,          
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
  
  const { filterConfigs, handleFiltersChange} = useFiltersHandler(searchParamKeys);

  const [ searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation();

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
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={handleFiltersChange}
      />
    </HStack>
  );
};
  

