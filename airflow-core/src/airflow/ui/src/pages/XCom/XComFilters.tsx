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
import { Box, HStack } from "@chakra-ui/react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

const {
  DAG_ID_PATTERN: DAG_ID_PATTERN_PARAM,
  KEY_PATTERN: KEY_PATTERN_PARAM,
  RUN_ID_PATTERN: RUN_ID_PATTERN_PARAM,
  TASK_ID_PATTERN: TASK_ID_PATTERN_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

export const XComFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation(["browse"]);

  const keyPattern = searchParams.get(KEY_PATTERN_PARAM);
  const dagIdPattern = searchParams.get(DAG_ID_PATTERN_PARAM);
  const runIdPattern = searchParams.get(RUN_ID_PATTERN_PARAM);
  const taskIdPattern = searchParams.get(TASK_ID_PATTERN_PARAM);

  const createFilterHandler = useCallback(
    (paramKey: string) => (value: string) => {
      if (value === "") {
        searchParams.delete(paramKey);
      } else {
        searchParams.set(paramKey, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleKeyFilterChange = createFilterHandler(KEY_PATTERN_PARAM);
  const handleDagIdFilterChange = createFilterHandler(DAG_ID_PATTERN_PARAM);
  const handleRunIdFilterChange = createFilterHandler(RUN_ID_PATTERN_PARAM);
  const handleTaskIdFilterChange = createFilterHandler(TASK_ID_PATTERN_PARAM);

  return (
    <HStack flexWrap="wrap" gap={4} paddingY="4px">
      <Box minW="200px">
        <SearchBar
          defaultValue={keyPattern ?? ""}
          hideAdvanced
          hotkeyDisabled={false}
          onChange={handleKeyFilterChange}
          placeHolder={translate("xcom.filters.keyFilter")}
        />
      </Box>
      <Box minW="200px">
        <SearchBar
          defaultValue={dagIdPattern ?? ""}
          hideAdvanced
          hotkeyDisabled={true}
          onChange={handleDagIdFilterChange}
          placeHolder={translate("xcom.filters.dagFilter")}
        />
      </Box>
      <Box minW="200px">
        <SearchBar
          defaultValue={runIdPattern ?? ""}
          hideAdvanced
          hotkeyDisabled={true}
          onChange={handleRunIdFilterChange}
          placeHolder={translate("xcom.filters.runIdFilter")}
        />
      </Box>
      <Box minW="200px">
        <SearchBar
          defaultValue={taskIdPattern ?? ""}
          hideAdvanced
          hotkeyDisabled={true}
          onChange={handleTaskIdFilterChange}
          placeHolder={translate("xcom.filters.taskIdFilter")}
        />
      </Box>
    </HStack>
  );
};
